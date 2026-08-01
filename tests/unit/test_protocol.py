"""Tests for rembus protocol messages and helpers."""

import logging
import pytest
import rembus.protocol as rp
import rembus.settings as rs
import rembus.core as rc
from rembus.router import Router, twin_up
from rembus.twin import WsTwin


class WrongMsg(rp.RembusMsg):
    """Class wiyh missing to_payload impl"""

    id: int


def test_no_impl():
    """Test missing to_payload"""
    msg = WrongMsg(id=1)
    with pytest.raises(RuntimeError):
        msg.to_payload(enc=rp.CBOR)


def test_bytes2id():
    """Test the bytes2id function for converting byte arrays to IDs."""
    byte_data = bytearray(range(rp.MSGID_SZ))  # 0x00 to 0x0F
    result = rp.bytes2id(byte_data)
    assert isinstance(result, int)

    with pytest.raises(ValueError):
        rp.bytes2id(bytearray(range(15)))

    with pytest.raises(ValueError):
        rp.bytes2id(bytearray(range(17)))


def test_types_str():
    """Test the string representation of protocol types."""
    assert str(rp.RembusTimeout()) == "request timeout"
    assert str(rp.RembusConnectionClosed()) == "connection down"

    error = rp.RembusError(rp.STS_METHOD_EXCEPTION, "foo")
    assert str(error) == "METHOD_EXCEPTION:foo"

    error_no_msg = rp.RembusError(rp.STS_ERROR)
    assert str(error_no_msg) == "internal error"


async def test_rembus_messages():
    """Test the string representation of rembus protocol messages."""
    router = Router("broker")
    await router.start()
    twin = WsTwin(rc.RbURL("twin"), router)
    await twin.start()
    for msg in [
        rp.AttestationMsg(id=1, cid="cid", signature=b"signature"),
        rp.IdentityMsg(id=2, cid="cid"),
    ]:
        logging.info(str(msg))

    await twin.close()


async def test_ip_not_resolved():
    """Test handling of unresolved IP addresses in the router."""

    class SocketMock:
        def __init__(self, remote_address):
            self.remote_address = (remote_address, 1234)

        async def close(self):
            pass

    router = Router("broker")
    await router.start()
    twin = WsTwin(rc.RbURL("twin"), router)
    await twin.start()

    twin.socket = SocketMock("1.2.3.4")
    await twin_up(twin)

    await twin.close()


def test_jsonrpc_request_invalid():
    """Test handling of invalid JSON-RPC request."""
    with pytest.raises(ValueError):
        rp.jsonrpc_request(
            '{"jsonrpc": "2.0", "method": "invalid_method"}',
            msg_id=1,
            params={})

def test_jsonrpc_parse_invalid():
    """Test handling of invalid JSON-RPC request."""
    with pytest.raises(ValueError):
        rp.jsonrpc_parse(
            '{"jsonrpc": "2.0", "id": 1, "method": "invalid_method"}',
        )


def test_jsonrpc_response_invalid():
    """Test handling of invalid JSON-RPC response."""
    with pytest.raises(ValueError):
        rp.jsonrpc_response(
            '{"jsonrpc": "2.0", "method": "invalid_method"}',
            msg_id=1,
            result={"type": "invalid"})


def test_env_bool():
    """Test the env_bool function for environment variable parsing."""
    import os

    os.environ["TEST_BOOL_TRUE"] = "true"
    os.environ["TEST_BOOL_FALSE"] = "false"
    os.environ["TEST_BOOL_YES"] = "yes"
    os.environ["TEST_BOOL_NO"] = "no"
    os.environ["TEST_BOOL_ON"] = "on"
    os.environ["TEST_BOOL_OFF"] = "off"
    os.environ["TEST_BOOL_1"] = "1"
    os.environ["TEST_BOOL_0"] = "0"

    assert rs.env_bool("TEST_BOOL_TRUE") is True
    assert rs.env_bool("TEST_BOOL_FALSE") is False
    assert rs.env_bool("TEST_BOOL_YES") is True
    assert rs.env_bool("TEST_BOOL_NO") is False
    assert rs.env_bool("TEST_BOOL_ON") is True
    assert rs.env_bool("TEST_BOOL_OFF") is False
    assert rs.env_bool("TEST_BOOL_1") is True
    assert rs.env_bool("TEST_BOOL_0") is False
    assert rs.env_bool("NON_EXISTENT_VAR", default=True) is True