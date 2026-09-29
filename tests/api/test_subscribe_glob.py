"""Test the rembus broker feature."""

import logging
import os
import rembus
import rembus.protocol as rp

logger = logging.getLogger(__name__)


def broker_handler(topic, x, y, ctx, node):
    logger.info("broker_handler called with topic=%s, x=%s, y=%s", topic, x, y)
    ctx[topic] = (x, y)

def sub_handler(topic, x, y, ctx, node):
    logger.info("sub_handler called with topic=%s, x=%s, y=%s", topic, x, y)
    ctx[topic] = (x, y)


def test_subscribe_glob():
    """Test subscribing with a glob pattern."""
    server_ctx = {}
    sub_ctx = {}
    x = 1
    y = 2

    server = rembus.node(port=8801)
    server.subscribe(broker_handler, topic="**")
    server.inject(server_ctx)

    srv_name = "srv"
    srv = rembus.node(f"ws://:8801/{srv_name}")


    sub = rembus.node("ws://:8801/mysub")
    sub.subscribe(sub_handler, topic="**")
    sub.inject(sub_ctx)

    cli = rembus.node("ws://:8801/cli")
    cli.publish("/myservice/abc", x, y)


    srv.close()
    cli.close()
    sub.close()
    server.close()
    assert server_ctx["/myservice/abc"] == (x, y)
    assert sub_ctx["/myservice/abc"] == (x, y)


