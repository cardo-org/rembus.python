"""Test the registration of rembus nodes authorized by an OPAQUE login."""

import os
import pytest
import rembus
import rembus.protocol as rp
import rembus.settings as rs
from tools.tenant import add_tenant_opaque

PIN = "11223344"
PORT = 8997


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
    add_tenant_opaque(".", PIN)

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

