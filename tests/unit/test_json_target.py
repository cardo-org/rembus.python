import json

import pytest

import rembus.protocol as rp


def test_direct_request_has_target_member():
    msg = rp.RpcReqMsg(id=1, topic="a/b", data=[1], target="node")
    pkt = json.loads(msg.to_payload(rp.JSON))
    assert pkt["method"] == "a/b"
    assert pkt["target"] == "node"


def test_untargeted_request_has_no_target_member():
    msg = rp.RpcReqMsg(id=1, topic="f", data=[])
    assert "target" not in json.loads(msg.to_payload(rp.JSON))


def test_parse_target():
    payload = '{"rembus":"1.0","id":"1","method":"a/b","params":[],"target":"n"}'
    msg = rp.jsonrpc_parse(payload)
    assert (msg.topic, msg.target) == ("a/b", "n")


@pytest.mark.parametrize("target", ["null", "5", "[]"])
def test_parse_target_null_or_invalid(target):
    payload = (
        '{"rembus":"1.0","id":"1","method":"f","params":[],"target":' + target + "}"
    )
    if target == "null":
        assert rp.jsonrpc_parse(payload).target is None
    else:
        with pytest.raises(ValueError):
            rp.jsonrpc_parse(payload)
