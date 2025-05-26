import logging
import os
import pytest
import rembus
import shutil
import ssl

import rembus.settings

def config_secure():
    data_dir = os.path.join("tests", "data")
    ca_dir = os.path.join(rembus.rembus_dir(), "ca")
    keystore_dir = rembus.settings.keystore_dir()
    if not os.path.exists(keystore_dir):
        os.makedirs(keystore_dir)

    if not os.path.exists(ca_dir):
        os.makedirs(ca_dir)

    shutil.copy(os.path.join(data_dir, "rembus-ca.crt"), ca_dir)
    shutil.copy(os.path.join(data_dir, "rembus.crt"), keystore_dir)
    shutil.copy(os.path.join(data_dir, "rembus.key"), keystore_dir)

def test_wss_fail():
    # missing private key and certificate
    try:
        server = rembus.node(port=8000, secure=True)
    except RuntimeError as e:
        logging.info(f"Expected error: {e}")
        assert True

def test_wss_ok():
    config_secure()
    server = rembus.node(port=8000, secure=True)

    rb = rembus.node("wss://127.0.0.1:8000/mycomponent")
    rb.close()

    server.close()

def test_wss_default_ca_notfound():
    fn = rembus.settings.rembus_ca()
    if os.path.exists(fn):
        os.remove(fn)

    server = rembus.node(port=8000, secure=True)
    with pytest.raises(Exception):
        rembus.node("wss://127.0.0.1:8000/mycomponent")

    server.close()

def test_wss_ca_notfound():
    config_secure()
    server = rembus.node(port=8000, secure=True)
    os.environ["HTTP_CA_BUNDLE"] = "not_found"
    with pytest.raises(ssl.SSLCertVerificationError):
        rembus.node("wss://127.0.0.1:8000/mycomponent")

    server.close()    

