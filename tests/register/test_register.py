import logging
import os
import pytest
import rembus
import rembus.protocol as rp
import time

import rembus.settings
import rembus.settings

logger = logging.getLogger(__name__)

name = "test_register"
pin = "11223344"

def test_register():
    # cleanup the secrets
    try:
        fn = rembus.settings.key_file(rembus.settings.DEFAULT_BROKER, name)
        logger.info(f"broker key file: {fn}")
        os.remove(fn)
    except FileNotFoundError:
            pass

    server = rembus.node(port=8000)
    rembus.register(name, pin)
    
    with pytest.raises(rp.RembusError):
        rembus.register("invalid_pin", "00000000")

    server.close()
    assert rembus.twin.isregistered(rembus.settings.DEFAULT_BROKER, name)

def test_register_ecdsa():
    myname = "test_register_ecdsa"
    # cleanup the secrets
    try:
        fn = rembus.settings.key_file(rembus.settings.DEFAULT_BROKER, myname)
        logger.info(f"broker key file: {fn}")
        os.remove(fn)
    except FileNotFoundError:
            pass

    server = rembus.node(port=8000)
    rembus.register(myname, pin, rp.SIG_ECDSA)
    assert rembus.twin.isregistered(rembus.settings.DEFAULT_BROKER, myname)
    
    rb = rembus.node(myname)
    rb.unregister()
    rb.close()

    server.close()

def test_register_already_provisioned():
    server = rembus.node(port=8000)
    with pytest.raises(rp.RembusError):
        rembus.register("test_register", "11223344")
    server.close()

def test_connect_authenticated():
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
    fn = rembus.settings.key_file(rembus.settings.DEFAULT_BROKER, name)
    # create an empty secret file to trigger a verify error 
    with open(fn, 'w') as f:
        pass

    server = rembus.node(port=8000)
    with pytest.raises(rp.RembusError):
        rembus.node("test_register")

    server.close()

def test_unregister():
    # cleanup the secrets
    try:
        fn = rembus.settings.key_file(rembus.settings.DEFAULT_BROKER, name)
        logger.info(f"broker key file: {fn}")
        os.remove(fn)
    except FileNotFoundError:
            pass

    server = rembus.node(port=8000)
    rembus.register(name, pin)

    rb = rembus.node(name)
    rb.unregister()
    rb.close()
    server.close()
