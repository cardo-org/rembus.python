"""Test the registration of rembus nodes authorized by an OPAQUE login."""

import os
import pytest
import rembus
import rembus.protocol as rp
import rembus.settings as rs

PIN = "11223344"
PORT = 8997
pytestmark = pytest.mark.usefixtures("clean_secrets")


def _cleanup(name):
    try:
        os.remove(rs.key_file(rs.DEFAULT_BROKER, name))
    except FileNotFoundError:
        pass


@pytest.mark.parametrize("enc", [rp.CBOR, rp.JSON])
def test_opaque_register(enc):
    """Register with the tenant pin verified by OPAQUE, without storing it."""
    name = f"test_opaque_{enc}"
    _cleanup(name)
    rembus.add_tenant_opaque(".", PIN)

    server = rembus.node(port=PORT)
    try:
        with pytest.raises(rp.RembusError):
            rembus.register(
                f"ws://:{PORT}/{name}", "00000000", enc=enc, opaque=True
            )
        assert not rp.isregistered(rs.DEFAULT_BROKER, name)

        rembus.register(f"ws://:{PORT}/{name}", PIN, enc=enc, opaque=True)
        assert rp.isregistered(rs.DEFAULT_BROKER, name)

        with pytest.raises(rp.RembusError):
            rembus.register(f"ws://:{PORT}/{name}", PIN, enc=enc, opaque=True)

        rb = rembus.node(f"ws://:{PORT}/{name}", enc=enc)
        rb.unregister()
        rb.close()
    finally:
        server.close()



@pytest.mark.parametrize("enc", [rp.CBOR, rp.JSON])
def test_opaque_rekey(enc):
    """Rekey authorized by OPAQUE (op 2) replaces the registered key."""
    name = f"test_opaque_rekey_{enc}"
    _cleanup(name)
    rembus.add_tenant_opaque(".", PIN)

    server = rembus.node(port=PORT)
    try:
        url = f"ws://:{PORT}/{name}"
        rembus.register(url, PIN, enc=enc, opaque=True)
        with open(rs.key_file(rs.DEFAULT_BROKER, name), "rb") as f:
            old = f.read()

        rb = rembus.node(url, enc=enc)
        with pytest.raises(rp.RembusError):
            rb.rekey("00000000", opaque=True)
        with open(rs.key_file(rs.DEFAULT_BROKER, name), "rb") as f:
            assert f.read() == old

        rb.rekey(PIN, rp.SIG_ECDSA, opaque=True)
        with open(rs.key_file(rs.DEFAULT_BROKER, name), "rb") as f:
            assert f.read() != old
        rb.close()

        rb = rembus.node(url, enc=enc)
        rb.unregister()
        rb.close()
    finally:
        server.close()


def test_opaque_rekey_unauthenticated():
    """op 2 needs a connection authenticated as the same cid."""
    name = "test_opaque_rekey_unauth"
    _cleanup(name)
    rembus.add_tenant_opaque(".", PIN)
    server = rembus.node(port=PORT)
    try:
        with pytest.raises(rp.RembusError):
            rembus.node(f"ws://:{PORT}/{name}").rekey(PIN, opaque=True)
    finally:
        server.close()


def test_opaque_unknown_tenant_same_stage1():
    """Stage 1 does not reveal whether the tenant exists (fake record)."""
    from rembus.opaque import fake_record  # pylint: disable=C0415

    assert isinstance(fake_record(rs.DEFAULT_BROKER), bytes)
    rembus.add_tenant_opaque(".", PIN)
    server = rembus.node(port=PORT)
    try:
        # Unknown tenant: fails in the finish step, like a wrong pin.
        with pytest.raises(rp.RembusError, match="wrong tenant/pin"):
            rembus.register(
                f"ws://:{PORT}/nosuch.test_x", PIN, opaque=True
            )
    finally:
        server.close()


def _fresh(name):
    _cleanup(name)
    return f"ws://:{PORT}/{name}"


def test_opaque_exchange_expired(monkeypatch):
    """A stage 2 after the exchange lifetime is rejected."""
    import rembus.router as rr  # pylint: disable=C0415

    monkeypatch.setattr(rr, "OPAQUE_TTL", 0.0)
    rembus.add_tenant_opaque(".", PIN)
    server = rembus.node(port=PORT)
    try:
        url = _fresh("test_opaque_expired")
        with pytest.raises(rp.RembusError):
            rembus.register(url, PIN, opaque=True)
        assert not rp.isregistered(rs.DEFAULT_BROKER, "test_opaque_expired")
    finally:
        server.close()


def test_opaque_too_many_exchanges(monkeypatch):
    """A stage 1 over the per-connection bound is rejected."""
    import rembus.router as rr  # pylint: disable=C0415

    monkeypatch.setattr(rr, "OPAQUE_MAX_EXCHANGES", 0)
    rembus.add_tenant_opaque(".", PIN)
    server = rembus.node(port=PORT)
    try:
        url = _fresh("test_opaque_toomany")
        with pytest.raises(rp.RembusError):
            rembus.register(url, PIN, opaque=True)
    finally:
        server.close()


def test_opaque_tenant_modes():
    """A tenant is either pin-proof or opaque-only, never both."""
    server = None
    try:
        # pin-proof tenant: OPAQUE fails (fake record), pin proof works.
        rembus.add_tenant(".", PIN)
        server = rembus.node(port=PORT)
        url = _fresh("test_mode_pin")
        with pytest.raises(rp.RembusError):
            rembus.register(url, PIN, opaque=True)
        assert not rp.isregistered(rs.DEFAULT_BROKER, "test_mode_pin")
        rembus.register(url, PIN)
        assert rp.isregistered(rs.DEFAULT_BROKER, "test_mode_pin")
        rb = rembus.node(url)
        rb.unregister()
        rb.close()
        server.close()

        # opaque-only tenant: the pin proof fails, OPAQUE works.
        # Changing the mode is refused while a secret of the old mode exists.
        with pytest.raises(ValueError):
            rembus.add_tenant_opaque(".", PIN)
        rembus.remove_tenant(".")
        rembus.add_tenant_opaque(".", PIN)
        server = rembus.node(port=PORT)
        url = _fresh("test_mode_opaque")
        with pytest.raises(rp.RembusError):
            rembus.register(url, PIN)
        assert not rp.isregistered(rs.DEFAULT_BROKER, "test_mode_opaque")
        rembus.register(url, PIN, opaque=True)
        rb = rembus.node(url)
        rb.unregister()
        rb.close()
    finally:
        if server:
            server.close()
