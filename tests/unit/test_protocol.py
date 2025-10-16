"""Tests for rembus protocol messages and helpers."""
import logging
import pytest
import rembus.protocol as rp
import rembus.core as rc


class WrongMsg(rp.RembusMsg):
    """Class wiyh missing to_payload impl"""
    id: int


def test_no_impl():
    """Test missing to_payload"""
    msg = WrongMsg(id=1)
    with pytest.raises(RuntimeError):
        msg.to_payload(enc=rp.CBOR)



def test_types_str():
    """Test the string representation of protocol types."""
    assert str(rp.RembusTimeout()) == 'request timeout'
    assert str(rp.RembusConnectionClosed()) == 'connection down'

    error = rp.RembusError(rp.STS_METHOD_EXCEPTION, "foo")
    assert str(error) == "METHOD_EXCEPTION:foo"

    error_no_msg = rp.RembusError(rp.STS_ERROR)
    assert str(error_no_msg) == "internal error"


async def test_rembus_messages():
    """Test the string representation of rembus protocol messages."""
    router = rc.Router("router")
    twin = rc.Twin(rc.RbURL("twin"), router)
    for msg in [
        rp.AttestationMsg(id=1, cid="cid", signature=b"signature"),
        rp.IdentityMsg(id=2, cid="cid"),
    ]:
        logging.info(str(msg))

    await twin.close()
