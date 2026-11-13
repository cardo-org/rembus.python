"""Registration secrets associated with a cid, with the tenant as fallback."""

import os
import pytest
import rembus
import rembus.protocol as rp
import rembus.settings as rs

PORT = 8996
pytestmark = pytest.mark.usefixtures("clean_secrets")
TENANT_PIN = "11223344"
CID_PIN = "99887766"
TENANT = "acme"

MODES = ["pin-proof", "opaque-only"]


def _add(mode, scope, name, pin):
    """Provision a secret in `mode` for a tenant or a cid."""
    fn = {
        ("pin-proof", "tenant"): rembus.add_tenant,
        ("opaque-only", "tenant"): rembus.add_tenant_opaque,
        ("pin-proof", "cid"): rembus.add_cid,
        ("opaque-only", "cid"): rembus.add_cid_opaque,
    }[(mode, scope)]
    fn(name, pin)


def _fresh(cid):
    try:
        os.remove(rs.key_file(rs.DEFAULT_BROKER, cid))
    except FileNotFoundError:
        pass
    return f"ws://:{PORT}/{cid}"


def _ok(url, pin, mode):
    rembus.register(url, pin, opaque=(mode == "opaque-only"))


def _fails(url, pin, mode):
    cid = url.rsplit("/", 1)[1]
    with pytest.raises(rp.RembusError):
        _ok(url, pin, mode)
    assert not rp.isregistered(rs.DEFAULT_BROKER, cid)


def _unregister(url):
    rb = rembus.node(url)
    rb.unregister()
    rb.close()


@pytest.mark.parametrize("mode", MODES)
def test_cid_secret_only(mode):
    """A cid secret works with no tenant secret, and only for that cid."""
    cid = "cid_only.acme"
    _add(mode, "cid", cid, CID_PIN)
    server = rembus.node(port=PORT)
    try:
        _fails(_fresh("other.acme"), CID_PIN, mode)
        url = _fresh(cid)
        _fails(url, "00000000", mode)
        _ok(url, CID_PIN, mode)
        assert rp.isregistered(rs.DEFAULT_BROKER, cid)
        _unregister(url)
    finally:
        server.close()


@pytest.mark.parametrize("mode", MODES)
def test_tenant_fallback(mode):
    """A cid without its own secret is resolved by the tenant secret."""
    _add(mode, "tenant", TENANT, TENANT_PIN)
    server = rembus.node(port=PORT)
    try:
        url = _fresh("fallback.acme")
        _fails(url, CID_PIN, mode)
        _ok(url, TENANT_PIN, mode)
        _unregister(url)
    finally:
        server.close()


@pytest.mark.parametrize("mode", MODES)
def test_cid_secret_wins_no_fallback(mode):
    """The tenant secret is not accepted for a cid that has its own."""
    cid = "wins.acme"
    _add(mode, "tenant", TENANT, TENANT_PIN)
    _add(mode, "cid", cid, CID_PIN)
    server = rembus.node(port=PORT)
    try:
        url = _fresh(cid)
        _fails(url, TENANT_PIN, mode)
        _ok(url, CID_PIN, mode)
        # the cid secret is kept by Register
        rb = rembus.node(url)
        with pytest.raises(rp.RembusError):
            rb.rekey(TENANT_PIN, opaque=(mode == "opaque-only"))
        rb.rekey(CID_PIN, rp.SIG_ECDSA, opaque=(mode == "opaque-only"))
        rb.close()
        # the same resolution after Rekey: the cid secret still applies
        rb = rembus.node(url)
        rb.rekey(CID_PIN, opaque=(mode == "opaque-only"))
        rb.close()
        _unregister(url)
    finally:
        server.close()


@pytest.mark.parametrize("mode", MODES)
def test_unregister_removes_cid_secret(mode):
    """After Unregister the cid is resolved by the tenant secret."""
    cid = "unreg.acme"
    _add(mode, "tenant", TENANT, TENANT_PIN)
    _add(mode, "cid", cid, CID_PIN)
    server = rembus.node(port=PORT)
    try:
        url = _fresh(cid)
        _ok(url, CID_PIN, mode)
        _unregister(url)
        _fails(url, CID_PIN, mode)
        _ok(url, TENANT_PIN, mode)
        _unregister(url)
    finally:
        server.close()


@pytest.mark.parametrize("mode", MODES)
def test_cid_secret_removal_keeps_key(mode):
    """Removing the cid secret keeps the key; Rekey then uses the tenant's."""
    cid = "removed.acme"
    _add(mode, "tenant", TENANT, TENANT_PIN)
    _add(mode, "cid", cid, CID_PIN)
    server = rembus.node(port=PORT)
    try:
        url = _fresh(cid)
        _ok(url, CID_PIN, mode)
    finally:
        server.close()
    rembus.remove_cid(cid)
    server = rembus.node(port=PORT)
    try:
        rb = rembus.node(url)
        with pytest.raises(rp.RembusError):
            rb.rekey(CID_PIN, opaque=(mode == "opaque-only"))
        rb.rekey(TENANT_PIN, opaque=(mode == "opaque-only"))
        rb.unregister()
        rb.close()
    finally:
        server.close()


def test_no_secret_fails():
    """No cid secret and no tenant secret: the request fails."""
    server = rembus.node(port=PORT)
    try:
        for mode in MODES:
            _fails(_fresh("nosecret.acme"), TENANT_PIN, mode)
    finally:
        server.close()


def test_mode_enforced_at_provisioning():
    """Secrets must match the mode of the tenant (REQ-AUTH-068)."""
    rembus.add_tenant(TENANT, TENANT_PIN)
    with pytest.raises(ValueError):
        rembus.add_cid_opaque("x.acme", CID_PIN)
    with pytest.raises(ValueError):
        rembus.add_tenant_opaque(TENANT, TENANT_PIN)
    with pytest.raises(ValueError):
        rembus.set_tenant_mode(TENANT, "opaque-only")
    rembus.add_cid("x.acme", CID_PIN)

    # a mode cannot change while a cid secret of the old mode exists
    rembus.remove_tenant(TENANT)
    with pytest.raises(ValueError):
        rembus.add_tenant_opaque(TENANT, TENANT_PIN)
    rembus.remove_cid("x.acme")
    rembus.add_tenant_opaque(TENANT, TENANT_PIN)

    # an explicit mode applies to a tenant holding no secret
    rembus.remove_tenant(TENANT)
    rembus.set_tenant_mode(TENANT, "opaque-only")
    with pytest.raises(ValueError):
        rembus.add_cid("y.acme", CID_PIN)
    rembus.add_cid_opaque("y.acme", CID_PIN)
    rembus.set_tenant_mode(TENANT, None)


def test_invalid_cid_rejected():
    with pytest.raises(ValueError):
        rembus.add_cid("bad:cid", CID_PIN)
