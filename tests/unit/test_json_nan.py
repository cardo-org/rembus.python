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



@pytest.mark.parametrize(
    "payload,expected",
    [
        ('{"id":5,"method":"f","params":[NaN]}', 5),
        ('{"id":7,"method":"f","params":[Infinity]}', 7),
        ('{"id":3,"method":"f","params":{"type":99}}', 3),
        ('{"method":"t","params":[1]}', None),
        ("{bad", None),
        ("[1]", None),
        ('{"id":3,"result":{}}', None),
    ],
)
def test_malformed_request_id(payload, expected):
    assert rp.jsonrpc_malformed_request_id(payload) == expected
