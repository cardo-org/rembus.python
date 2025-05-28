import logging
import pytest
import rembus.protocol as rp
import rembus.core as rc


def test_bytes2id():
    byte_data = bytearray(range(16))  # 0x00 to 0x0F
    result = rp.bytes2id(byte_data)
    assert isinstance(result, int)

    with pytest.raises(ValueError):
        rp.bytes2id(bytearray(range(15)))

    with pytest.raises(ValueError):
        rp.bytes2id(bytearray(range(17)))


def test_types_str():
    assert str(rp.RembusTimeout()) == 'request timeout'
    assert str(rp.RembusConnectionClosed()) == 'connection down'

    error = rp.RembusError(rp.STS_METHOD_EXCEPTION, "foo")
    assert str(error) == "METHOD_EXCEPTION:foo"

    error_no_msg = rp.RembusError(rp.STS_ERROR)
    assert str(error_no_msg) == "internal error"


async def test_rembus_messages():
    router = rc.Router("router")
    twin = rc.Twin(rc.RbURL("twin"), router)
    for msg in [
        rp.AttestationMsg(
            twin, payload=[rp.TYPE_ATTESTATION, 1, "cid", "signature"]),
        rp.IdentityMsg(twin, payload=[rp.TYPE_IDENTITY, 1, "cid"]),
    ]:
        logging.info(str(msg))

    await twin.close()
