"""
OPAQUE (RFC 9807) support for the tenant pin of the Register operation.

The broker stores for each tenant an OPAQUE *password file* instead of the
plain pin, plus a broker wide *server setup* (OPRF seed and server keypair)
kept in a file readable only by the broker. A component proves that it knows
the tenant pin with an OPAQUE login and authorizes the public key to register
with a MAC computed under the resulting session key.
"""

import hashlib
import hmac
import os
import opaque_ke_py as ok
import rembus.settings as rs


def setup_file(router_id: str) -> str:
    """Return the path of the server setup file of the broker."""
    return os.path.join(rs.broker_dir(router_id), "opaque_setup")


def load_setup(router_id: str) -> ok.ServerSetupData:
    """Load the server setup, creating it (mode 0600) on first use."""
    fn = setup_file(router_id)
    try:
        with open(fn, "rb") as f:
            return ok.ServerSetupData.from_bytes(f.read())
    except FileNotFoundError:
        pass

    os.makedirs(os.path.dirname(fn), exist_ok=True)
    setup = ok.server_setup()
    try:
        fd = os.open(fn, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        # Created concurrently by another process.
        return load_setup(router_id)
    with os.fdopen(fd, "wb") as f:
        f.write(setup.to_bytes())
    return setup


def enroll(router_id: str, tenant: str, pin: str) -> bytes:
    """Return the password file of the tenant, to be stored by the broker.

    The registration runs locally, so it must be executed where the server
    setup file is readable (see :func:`load_setup`).
    """
    setup = load_setup(router_id)
    password = pin.encode()
    start = ok.client_registration_start(password)
    resp = ok.server_registration_start(
        setup, start.get_message(), tenant.encode()
    )
    finish = ok.client_registration_finish(
        password, start.get_state(), resp.get_message()
    )
    return ok.server_registration_finish(
        finish.get_message()
    ).get_password_file()


LABELS = {1: b"rembus-register", 2: b"rembus-rekey"}


def key_mac(
    session_key: bytes, cid: str, key_type: int, pubkey: bytes, op: int = 1
) -> bytes:
    """MAC binding the public key and the operation to the OPAQUE session.

    `op` is 1 (Register, label ``rembus-register``) or 2 (Rekey, label
    ``rembus-rekey``).
    """
    msg = LABELS[op] + hashlib.sha256(
        cid.encode() + bytes([key_type]) + pubkey
    ).digest()
    return hmac.new(session_key, msg, hashlib.sha256).digest()


def fake_record(router_id: str) -> bytes:
    """Return a record of no tenant, used to answer stage 1 for a tenant
    without a record so that the reply does not reveal whether it exists.

    The record belongs to a random pin that is discarded, so the exchange
    can never complete; it is created once per process.
    """
    global _FAKE  # pylint: disable=global-statement
    if _FAKE is None:
        _FAKE = enroll(router_id, "-fake-", os.urandom(32).hex())
    return _FAKE


_FAKE: bytes | None = None
