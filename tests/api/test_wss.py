"""Tests for WebSocket Secure (WSS) connections in rembus."""

import os
import shutil
import ssl
import pytest
import rembus
import rembus.settings
import time


def config_secure():
    """
    Configure the rembus settings for secure WSS connections.
    This function sets up the necessary directories and copies
    the required certificate and key files for secure communication.
    """
    data_dir = os.path.join("tests", "cfg")
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
    """
    Test the failure condition for WSS connections when the
    necessary private key and certificate are not available.
    This should raise a RuntimeError indicating that the
    secure connection cannot be established.
    """
    try:
        rembus.node(port=8900, secure=True)
    except RuntimeError:
        assert True


def test_wss_ok():
    """
    Test the successful establishment of a WSS connection.
    This function configures the rembus settings for secure
    connections, starts a rembus server, and then attempts
    to create a WSS node. It should not raise any exceptions
    and should successfully close the connection.
    """
    config_secure()
    server = rembus.node(port=8900, secure=True)

    rb = rembus.node("wss://127.0.0.1:8900/mycomponent")
    rb.close()

    server.close()


def test_wss_default_ca_notfound():
    """
    Test the condition where the default CA file is not found.
    This function removes the default CA file, and then attempts
    to create a WSS node. It should raise an exception indicating
    that the CA file is not found.
    """
    fn = rembus.settings.rembus_ca()
    if os.path.exists(fn):
        os.remove(fn)

    server = rembus.node(port=8900, secure=True)
    with pytest.raises(Exception):
        rembus.node("wss://127.0.0.1:8900/mycomponent")

    server.close()


def test_wss_ca_notfound():
    """
    Test the condition where a specific CA bundle is not found.
    This function sets an environment variable for the CA bundle
    and then attempts to create a WSS node. It should raise an
    exception indicating that the CA bundle file is not found.
    """
    config_secure()
    server = rembus.node(port=8900, secure=True)
    os.environ["HTTP_CA_BUNDLE"] = "not_found"
    with pytest.raises(ssl.SSLCertVerificationError):
        rembus.node("wss://127.0.0.1:8900/mycomponent")

    server.close()
    os.environ.pop("HTTP_CA_BUNDLE")
