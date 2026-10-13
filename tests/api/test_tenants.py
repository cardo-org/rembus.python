"""Test that pub/sub and rpc are isolated between tenants."""

import time
import pytest
import rembus
import rembus.protocol as rp


@pytest.mark.parametrize("policy", ["first_up", "round_robin", "less_busy"])
def test_rpc_tenants(policy):
    """RPC requests are routed only to exposers of the requestor tenant."""
    server = rembus.node(port=8802, policy=policy)
    org = rembus.node("ws://:8802/exposer.org")
    org.expose(lambda x: x + 1, topic="myservice")
    com = rembus.node("ws://:8802/exposer.com")

    cli_org = rembus.node("ws://:8802/rpc_client.org")
    assert cli_org.rpc("myservice", 1) == 2

    cli_com = rembus.node("ws://:8802/rpc_client.com")
    with pytest.raises(rp.RembusError):
        cli_com.rpc("myservice", 1)

    for c in (cli_org, cli_com, org, com, server):
        c.close()


def test_publish_tenants():
    """Messages are delivered only to subscribers of the publisher tenant."""
    server = rembus.node(port=8803)
    ctx = {}

    def mytopic(data, name):
        ctx[name] = data

    sub_org = rembus.node("ws://:8803/sub.org")
    sub_org.subscribe(lambda data: mytopic(data, "org"), topic="mytopic")
    sub_org.reactive()
    sub_com = rembus.node("ws://:8803/sub.com")
    sub_com.subscribe(lambda data: mytopic(data, "com"), topic="mytopic")
    sub_com.reactive()

    pub_org = rembus.node("ws://:8803/pub.org")
    pub_org.publish("mytopic", "hello world")
    time.sleep(0.5)

    assert ctx.get("org") == "hello world"
    assert "com" not in ctx

    for c in (pub_org, sub_org, sub_com, server):
        c.close()
