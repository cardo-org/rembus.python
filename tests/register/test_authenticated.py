"""Test the authenticated connection mode of the router."""

import os
import pytest
import rembus
import rembus.protocol as rp
import rembus.settings as rs
from tools.tenant import add_tenant

def echo(x):
    return x


NAME = "test_authenticated"
PIN = "11223344"
PORT = 8991


def test_authenticated_mode():
    """Only authenticated components can send commands."""
    add_tenant(".", PIN)
    try:
        os.remove(rs.key_file(rs.DEFAULT_BROKER, NAME))
    except FileNotFoundError:
        pass

    server = rembus.node(port=PORT, authenticated=True)

    # Registration is allowed without authentication.
    rembus.register(f"ws://:{PORT}/{NAME}", PIN)

    # The anonymous mode is not allowed.
    with pytest.raises(ValueError):
        rembus.node(f"ws://:{PORT}", authenticated=True)

    # An unregistered component cannot authenticate.
    with pytest.raises(rp.RembusError):
        rembus.node(f"ws://:{PORT}/unknown_cmp", authenticated=True)

    # The registered component is authenticated.
    cmp = rembus.node(f"ws://:{PORT}/{NAME}", authenticated=True)
    server.expose(echo)
    assert cmp.rpc("echo", 1) == 1
    cmp.unregister()
    cmp.close()
    server.close()


def test_anonymous_component_is_disconnected():
    """A component that does not authenticate cannot send commands."""
    server = rembus.node(port=PORT, authenticated=True)
    try:
        server.expose(echo)
        cmp = rembus.node(f"ws://:{PORT}")
        try:
            with pytest.raises((rp.RembusTimeout, rp.RembusConnectionClosed)):
                cmp.rpc("echo", 1)
        finally:
            cmp.close()
    finally:
        server.close()
