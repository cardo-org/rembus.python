"""Pub/Sub JSON messages use `topic`, RPC requests use `method`."""

import json

from rembus.protocol import (
    QOS1,
    JSON,
    PubSubMsg,
    RpcReqMsg,
    jsonrpc_parse,
)


def test_pub_uses_topic():
    """QoS0 and QoS1 publish encode `topic` and round-trip."""
    for flags in (0, QOS1):
        msg = PubSubMsg(topic="t", data=[1], flags=flags, id=5 if flags else None)
        payload = msg.to_payload(JSON)
        pkt = json.loads(payload)
        assert "topic" in pkt and "method" not in pkt
        out = jsonrpc_parse(payload)
        assert isinstance(out, PubSubMsg)
        assert out.topic == "t" and out.data == [1]


def test_rpc_still_uses_method():
    """RPC requests keep `method`."""
    payload = RpcReqMsg(id=7, topic="f", data=[1]).to_payload(JSON)
    assert "method" in json.loads(payload)
    assert isinstance(jsonrpc_parse(payload), RpcReqMsg)


def test_legacy_pub_with_method_rejected():
    """A Pub notification using `method` is not accepted."""
    import pytest

    with pytest.raises(ValueError):
        jsonrpc_parse('{"method": "t", "params": [1]}')


def test_json_ids_are_strings():
    """JSON ids are sent as decimal strings and parsed back to integers."""
    mid = 2**63 + 1
    payload = RpcReqMsg(id=mid, topic="f", data=[1]).to_payload(JSON)
    assert json.loads(payload)["id"] == str(mid)
    assert jsonrpc_parse(payload).id == mid


def test_slot_pub_not_supported_over_json():
    """Slot publish is not supported with the JSON encoding."""
    import pytest

    with pytest.raises(ValueError):
        PubSubMsg(topic="t", data=1, slot=10).to_payload(JSON)

    payload = (
        '{"topic": "t", "id": "5", '
        '"params": {"type": 80, "data": 1}}'
    )
    with pytest.raises(ValueError):
        jsonrpc_parse(payload)
