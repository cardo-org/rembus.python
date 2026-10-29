import pytest

from rembus import protocol as rp


@pytest.mark.parametrize("v", [float("nan"), float("inf"), float("-inf")])
def test_json_dumps_rejects_non_finite(v):
    with pytest.raises(ValueError):
        rp.json_dumps([1, {"a": v}])


def test_jsonrpc_encode_rejects_nan():
    msg = rp.RpcReqMsg(id=1, topic="f", data=[float("nan")])
    with pytest.raises(ValueError):
        msg.to_payload(rp.JSON)


def test_json_dumps_finite():
    assert rp.json_dumps([1.5]) == b"[1.5]"


def test_json_dumps_bytes_as_base64():
    assert rp.json_dumps({"a": b"\x01\x02\x03", "b": [b""]}) == (
        b'{"a":"AQID","b":[""]}'
    )
    assert rp.json_dumps(bytearray(b"\x01")) == b'"AQ=="'


def test_json_dumps_unsupported_type_still_fails():
    with pytest.raises(TypeError):
        rp.json_dumps(object())


def test_jsonrpc_encode_bytes():
    msg = rp.RpcReqMsg(id=1, topic="f", data=[b"\x01\x02\x03"])
    assert '"AQID"' in msg.to_payload(rp.JSON)
