import json
import time
import pytest
import rembus


def my_service(x, y):
    return x+y


def test_jsonrpc_rpc(server):
    rb = rembus.node("mynode", enc=rembus.JSON)
    rb.expose(my_service)
    res = rb.rpc("rid")
    assert res == "broker"
    rb.close()


def test_jsonrpc_publish(server):
    rb = rembus.node("mynode", enc=rembus.JSON)
    rb.publish("my_topic")
    rb.publish("my_topic", slot=10000)
    rb.publish("my_topic", qos=rembus.protocol.QOS1)
    rb.publish("my_topic", qos=rembus.protocol.QOS2)
    time.sleep(1)
    rb.close()


def test_jsonrpc_error(server):
    rb = rembus.node("mynode", enc=rembus.JSON)
    with pytest.raises(rembus.protocol.RembusError):
        rb.rpc("unknown_service")
    rb.close()


def test_jsonrpc_invalid_request():
    rb = rembus.node("mynode", enc=rembus.JSON)
    pkt = {
        "jsorpc": "2.0",
        "id": 1234,
        "params": {}
    }

    # Server-side throws an exception and close the connection.
    # The client detect the connection down and reconnect.
    rb._runner.run(rb._rb._send(json.dumps(pkt)))
    rb.close()


def test_jsonrpc_invalid_response():
    rb = rembus.node("mynode", enc=rembus.JSON)
    pkt = {
        "jsorpc": "2.0",
        "id": 1234,
        "result": {"type": 999}  # the type field has an invalid value
    }

    # Server-side throws an exception and close the connection.
    # The client detect the connection down and reconnect.
    rb._runner.run(rb._rb._send(json.dumps(pkt)))
    rb.close()


def test_jsonrpc_invalid_payload():
    rb = rembus.node("mynode", enc=rembus.JSON)
    pkt = {
        "jsorpc": "2.0",
        "id": 1234,
        # missing params, result or errors property
    }

    # Server-side throws an exception and close the connection.
    # The client detect the connection down and reconnect.
    rb._runner.run(rb._rb._send(json.dumps(pkt)))
    rb.close()
