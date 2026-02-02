"""Test the registration and unregistration of rembus nodes."""
import logging
import os
import pytest
import rembus
import rembus.protocol as rp
import rembus.settings
from tools.tenant import add_tenant

logger = logging.getLogger(__name__)
NAME = "test_register"
PIN = "11223344"


def test_add_tenant():
    # Setup tenant settings
    add_tenant(".", "11223344")


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
    with pytest.raises(rp.RembusError):
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
    with open(fn, 'w', encoding="utf-8") as _:
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
