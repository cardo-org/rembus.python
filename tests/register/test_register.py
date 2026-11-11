"""Test the registration and unregistration of rembus nodes."""

import logging
import os
import pytest
import rembus
import rembus.protocol as rp
import rembus.settings

logger = logging.getLogger(__name__)
NAME = "test_register"
PIN = "11223344"


def test_add_tenant():
    # Setup tenant settings
    rembus.add_tenant(".", "11223344")


def test_register():
    """Test the registration of a rembus node."""
    # cleanup the secrets
    try:
        fn = rembus.settings.key_file(rembus.settings.DEFAULT_BROKER, NAME)
        logger.info("broker key file: %s", fn)
        os.remove(fn)
    except FileNotFoundError:
        pass

    server = rembus.node(port=8990)
    rembus.register(f"ws://:8990/{NAME}", PIN)

    with pytest.raises(rp.RembusError):
        rembus.register("ws://:8990/invalid_pin", "00000000")

    server.close()
    assert rp.isregistered(rembus.settings.DEFAULT_BROKER, NAME)


def test_register_ecdsa():
    """Test the registration of a rembus node with an ECDSA key."""
    myname = "test_register_ecdsa"
    # cleanup the secrets
    try:
        fn = rembus.settings.key_file(rembus.settings.DEFAULT_BROKER, myname)
        os.remove(fn)
    except FileNotFoundError:
        pass

    server = rembus.node(port=8990)
    rembus.register(f"ws://:8990/{myname}", PIN, rp.SIG_ECDSA)
    assert rp.isregistered(rembus.settings.DEFAULT_BROKER, myname)

    rb = rembus.node(f"ws://:8990/{myname}")
    rb.unregister()
    rb.close()

    server.close()


def test_register_invalid_key_type():
    """The broker rejects a Register with a key type other than 1 or 2."""
    server = rembus.node(port=8990)
    with pytest.raises(rp.RembusError):
        rembus.register("ws://:8990/test_register_badtype", PIN, scheme=9)
    server.close()
    assert not rp.isregistered(
        rembus.settings.DEFAULT_BROKER, "test_register_badtype"
    )


def test_register_already_provisioned():
    """
    Test the error condition when trying to register
    an already provisioned node.
    """
    server = rembus.node(port=8990)
    with pytest.raises(rp.RembusError):
        rembus.register(f"ws://:8990/{NAME}", "11223344")
    server.close()


def test_connect_authenticated():
    """Test connecting to a registered rembus node."""
    server = rembus.node(port=8990)
    rb = rembus.node(f"ws://:8990/{NAME}")
    rid = rb.rpc("rid")
    assert rid == rembus.settings.DEFAULT_BROKER

    # already connected
    rembus.node(f"ws://:8990/{NAME}")

    rb.close()
    server.close()


def test_jsonrpc_connect_authenticated():
    """Test connecting to a registered rembus node with JSON-RPC."""
    server = rembus.node(port=8990)
    rb = rembus.node(f"ws://:8990/{NAME}", enc=rembus.JSON)
    rid = rb.rpc("rid")
    assert rid == rembus.settings.DEFAULT_BROKER

    rb.close()
    server.close()


def test_verify_error():
    """Test the error condition when the secret file is wrong."""
    fn = rembus.settings.key_file(rembus.settings.DEFAULT_BROKER, NAME)
    # create an empty secret file to trigger a verify error
    with open(fn, "w", encoding="utf-8") as _:
        pass

    server = rembus.node(port=8990)
    with pytest.raises(rp.RembusError):
        rembus.node(f"ws://:8990/{NAME}")

    server.close()


def test_unregister():
    """Test the unregistration of a rembus node."""
    # cleanup the secrets
    try:
        fn = rembus.settings.key_file(rembus.settings.DEFAULT_BROKER, NAME)
        os.remove(fn)
    except FileNotFoundError:
        pass

    server = rembus.node(port=8990)
    rembus.register(f"ws://:8990/{NAME}", PIN)

    rb = rembus.node(f"ws://:8990/{NAME}")
    rb.unregister()
    rb.close()
    server.close()


def test_unregister_unauthenticated():
    """Unregister is rejected for a component that is not authenticated."""
    server = rembus.node(port=8990)
    rb = rembus.node(f"ws://:8990/{NAME}_free")
    with pytest.raises(rp.RembusError):
        rb.unregister()
    rb.close()
    server.close()


def test_json_register():
    """Test the registration of a rembus node using JSON-RPC."""
    # cleanup the secrets
    try:
        fn = rembus.settings.key_file(rembus.settings.DEFAULT_BROKER, NAME)
        logger.info("broker key file: %s", fn)
        os.remove(fn)
    except FileNotFoundError:
        pass

    server = rembus.node(port=8990)
    rembus.register(f"ws://:8990/{NAME}", PIN, enc=rembus.JSON)
    assert rp.isregistered(rembus.settings.DEFAULT_BROKER, NAME)

    rb = rembus.node(f"ws://:8990/{NAME}", enc=rembus.JSON)
    rb.unregister()
    rb.close()
    server.close()


REKEY_NAME = "test_rekey"
REKEY_PORT = 8992


def _secret_path(cid):
    return os.path.join(rembus.settings.rembus_dir(), cid, ".secret")


def _read(fn):
    with open(fn, "rb") as f:
        return f.read()


def _pubkey_file(cid):
    return rembus.settings.key_file(rembus.settings.DEFAULT_BROKER, cid)


def _fresh_registration(cid):
    try:
        os.remove(_pubkey_file(cid))
    except FileNotFoundError:
        pass
    rembus.register(f"ws://:{REKEY_PORT}/{cid}", PIN)


def _rekey_raw(rb, cid, pin, pubkey, keytype=rp.SIG_RSA):
    """Send a Rekey packet with arbitrary fields and wait for the response."""

    async def go():
        return await rb._rb._pin_exchange(
            rp.RekeyMsg, cid, pin, pubkey, keytype
        )

    return rb.exec(go)


@pytest.mark.parametrize("scheme", [rp.SIG_RSA, rp.SIG_ECDSA])
def test_rekey(scheme):
    """The new key replaces the old one: only the new key authenticates."""
    rembus.add_tenant(".", PIN)
    server = rembus.node(port=REKEY_PORT)
    _fresh_registration(REKEY_NAME)
    old_secret = _read(_secret_path(REKEY_NAME))
    old_pub = _read(_pubkey_file(REKEY_NAME))

    rb = rembus.node(f"ws://:{REKEY_PORT}/{REKEY_NAME}")
    rb.rekey(PIN, scheme)
    # the connection stays open
    assert rb.rpc("rid") == rembus.settings.DEFAULT_BROKER
    rb.close()

    assert _read(_pubkey_file(REKEY_NAME)) != old_pub
    assert _read(_secret_path(REKEY_NAME)) != old_secret
    # a single key file remains
    keydir = os.path.dirname(_pubkey_file(REKEY_NAME))
    assert (
        len([f for f in os.listdir(keydir) if f.startswith(REKEY_NAME + ".")])
        == 1
    )

    rb = rembus.node(f"ws://:{REKEY_PORT}/{REKEY_NAME}")
    assert rb.rpc("rid") == rembus.settings.DEFAULT_BROKER
    rb.close()

    # the old private key no longer authenticates
    new_secret = _read(_secret_path(REKEY_NAME))
    with open(_secret_path(REKEY_NAME), "wb") as f:
        f.write(old_secret)
    with pytest.raises(rp.RembusError):
        rembus.node(f"ws://:{REKEY_PORT}/{REKEY_NAME}")
    with open(_secret_path(REKEY_NAME), "wb") as f:
        f.write(new_secret)
    server.close()


def test_rekey_failures_keep_old_key():
    """Wrong pin, unregistered cid, bad pubkey and cid mismatch fail (10)."""
    rembus.add_tenant(".", PIN)
    server = rembus.node(port=REKEY_PORT)
    _fresh_registration(REKEY_NAME)
    old_pub = _read(_pubkey_file(REKEY_NAME))
    good = rp.pem_public_key(rp.rsa_private_key())

    rb = rembus.node(f"ws://:{REKEY_PORT}/{REKEY_NAME}")

    # wrong pin (client API)
    with pytest.raises(rp.RembusError):
        rb.rekey("00000000")

    cases = [
        (REKEY_NAME, "00000000", good, rp.SIG_RSA),  # wrong pin
        (REKEY_NAME, PIN, b"not a key", rp.SIG_RSA),  # invalid pubkey
        (REKEY_NAME, PIN, b"", rp.SIG_RSA),  # empty pubkey
        (REKEY_NAME, PIN, good, rp.SIG_ECDSA),  # type mismatch
        (REKEY_NAME, PIN, good, 9),  # invalid type
        ("other_cid", PIN, good, rp.SIG_RSA),  # cid != authenticated cid
    ]
    for cid, pin, pub, kt in cases:
        res = _rekey_raw(rb, cid, pin, pub, kt)
        assert res.status == rp.STS_GENERIC_ERROR, (cid, kt)
        assert _read(_pubkey_file(REKEY_NAME)) == old_pub

    # connection still open
    assert rb.rpc("rid") == rembus.settings.DEFAULT_BROKER
    rb.close()

    # the old key still works
    rb = rembus.node(f"ws://:{REKEY_PORT}/{REKEY_NAME}")
    rb.unregister()
    rb.close()
    server.close()


def test_rekey_unauthenticated_or_unregistered():
    """An anonymous component cannot rekey, nor can an unregistered cid."""
    rembus.add_tenant(".", PIN)
    server = rembus.node(port=REKEY_PORT)
    rb = rembus.node(f"ws://:{REKEY_PORT}/{REKEY_NAME}_free")
    with pytest.raises(rp.RembusError):
        rb.rekey(PIN)
    rb.close()
    assert not rp.isregistered(
        rembus.settings.DEFAULT_BROKER, f"{REKEY_NAME}_free"
    )
    server.close()


def _register_raw(rb, cid, pubkey, keytype):
    """Send a Register packet with arbitrary key fields."""

    async def go():
        return await rb._rb._pin_exchange(
            rp.RegisterMsg, cid, PIN, pubkey, keytype
        )

    return rb.exec(go)


def test_register_invalid_keys():
    """Register rejects keys that do not match the declared type."""
    from cryptography.hazmat.primitives.asymmetric import ec, rsa

    cid = "test_register_badkey"
    rembus.add_tenant(".", PIN)
    server = rembus.node(port=REKEY_PORT)
    rb = rembus.node(f"ws://:{REKEY_PORT}/{cid}")

    short_rsa = rp.pem_public_key(
        rsa.generate_private_key(public_exponent=65537, key_size=1024)
    )
    p384 = rp.pem_public_key(ec.generate_private_key(ec.SECP384R1()))
    good_rsa = rp.pem_public_key(rp.rsa_private_key())
    good_ec = rp.pem_public_key(rp.ecdsa_private_key())

    cases = [
        (b"garbage", rp.SIG_RSA),
        (short_rsa, rp.SIG_RSA),
        (p384, rp.SIG_ECDSA),
        (good_rsa, rp.SIG_ECDSA),
        (good_ec, rp.SIG_RSA),
    ]
    for pub, kt in cases:
        res = _register_raw(rb, cid, pub, kt)
        assert res.status == rp.STS_GENERIC_ERROR
        assert not rp.isregistered(rembus.settings.DEFAULT_BROKER, cid)

    res = _register_raw(rb, cid, good_ec, rp.SIG_ECDSA)
    assert res.status == rp.STS_OK
    rb.close()
    os.remove(_pubkey_file(cid))
    server.close()


def test_legacy_key_file_compat():
    """A legacy key file is still honored and migrated by Rekey."""
    cid = "test_legacy_key"
    rembus.add_tenant(".", PIN)
    server = rembus.node(port=REKEY_PORT)
    _fresh_registration(cid)
    canonical = _pubkey_file(cid)
    assert canonical.endswith(".pub")
    legacy = canonical[: -len(".pub")] + ".rsa.pem"
    os.rename(canonical, legacy)
    assert _pubkey_file(cid) == legacy

    rb = rembus.node(f"ws://:{REKEY_PORT}/{cid}")
    rb.rekey(PIN)
    rb.close()
    assert _pubkey_file(cid) == canonical
    assert not os.path.exists(legacy)

    rb = rembus.node(f"ws://:{REKEY_PORT}/{cid}")
    rb.unregister()
    rb.close()
    server.close()
