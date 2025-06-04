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


def test_register():
    """Test the registration of a rembus node."""
    # cleanup the secrets
    try:
        fn = rembus.settings.key_file(rembus.settings.DEFAULT_BROKER, NAME)
        logger.info("broker key file: %s", fn)
        os.remove(fn)
    except FileNotFoundError:
        pass

    server = rembus.node(port=8000)
    rembus.register(NAME, PIN)

    with pytest.raises(rp.RembusError):
        rembus.register("invalid_pin", "00000000")

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

    server = rembus.node(port=8000)
    rembus.register(myname, PIN, rp.SIG_ECDSA)
    assert rp.isregistered(rembus.settings.DEFAULT_BROKER, myname)

    rb = rembus.node(myname)
    rb.unregister()
    rb.close()

    server.close()


def test_register_already_provisioned():
    """
    Test the error condition when trying to register
    an already provisioned node.
    """
    server = rembus.node(port=8000)
    with pytest.raises(rp.RembusError):
        rembus.register("test_register", "11223344")
    server.close()


def test_connect_authenticated():
    """Test connecting to a registered rembus node."""
    server = rembus.node(port=8000)
    rb = rembus.node("test_register")
    rid = rb.rpc("rid")
    assert rid == rembus.settings.DEFAULT_BROKER

    # already connected
    with pytest.raises(rp.RembusError):
        rembus.node("test_register")

    rb.close()
    server.close()


def test_verify_error():
    """Test the error condition when the secret file is wrong."""
    fn = rembus.settings.key_file(rembus.settings.DEFAULT_BROKER, NAME)
    # create an empty secret file to trigger a verify error
    with open(fn, 'w', encoding="utf-8") as _:
        pass

    server = rembus.node(port=8000)
    with pytest.raises(rp.RembusError):
        rembus.node("test_register")

    server.close()


def test_unregister():
    """Test the unregistration of a rembus node."""
    # cleanup the secrets
    try:
        fn = rembus.settings.key_file(rembus.settings.DEFAULT_BROKER, NAME)
        os.remove(fn)
    except FileNotFoundError:
        pass

    server = rembus.node(port=8000)
    rembus.register(NAME, PIN)

    rb = rembus.node(NAME)
    rb.unregister()
    rb.close()
    server.close()
