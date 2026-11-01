"""Pub/Sub message delivery rules for wildcard subscribers."""

import time

import pytest
import rembus as rb
import rembus.protocol as rp

PORT = 8811


def wait_for(cond, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.05)
    return cond()


@pytest.fixture
def broker():
    b = rb.node(port=PORT)
    yield b
    b.close()


def test_wildcard_delivery_rules(broker):
    got_sub, got_pub, got_nonreactive = {}, {}, {}

    def make(ctx):
        def handler(topic, x, ctx, node):
            ctx[topic] = x

        return handler

    sub = rb.node(f"ws://:{PORT}/md_sub")
    sub.subscribe(make(got_sub), topic="md/*")
    sub.inject(got_sub)
    sub.reactive()

    # Subscriber that is both publisher and wildcard subscriber: no echo.
    both = rb.node(f"ws://:{PORT}/md_both")
    both.subscribe(make(got_pub), topic="md/*")
    both.inject(got_pub)
    both.reactive()

    # Not reactive: receives nothing.
    quiet = rb.node(f"ws://:{PORT}/md_quiet")
    quiet.subscribe(make(got_nonreactive), topic="md/*")
    quiet.inject(got_nonreactive)

    both.publish("md/a", 1)
    assert wait_for(lambda: got_sub.get("md/a") == 1)
    time.sleep(0.3)
    assert got_pub == {}
    assert got_nonreactive == {}

    for c in (sub, both, quiet):
        c.close()


def test_wildcard_qos_delivery(broker):
    got = {}

    def handler(topic, x, ctx, node):
        ctx[topic] = x

    sub = rb.node(f"ws://:{PORT}/md_qsub")
    sub.subscribe(handler, topic="q/*")
    sub.inject(got)
    sub.reactive()

    pub = rb.node(f"ws://:{PORT}/md_qpub")
    pub.publish("q/a", 7, qos=rp.QOS1)
    assert wait_for(lambda: got.get("q/a") == 7)

    sub.close()
    pub.close()


def test_invalid_topic_publish_dropped(broker):
    got = {}

    def handler(topic, x, ctx, node):
        ctx[topic] = x

    sub = rb.node(f"ws://:{PORT}/md_isub")
    sub.subscribe(handler, topic="**")
    sub.inject(got)
    sub.reactive()

    pub = rb.node(f"ws://:{PORT}/md_ipub")
    pub.publish("a/*", 1)
    pub.publish("__reserved__", 1)
    pub.publish("ok/topic", 2)
    assert wait_for(lambda: got.get("ok/topic") == 2)
    assert "a/*" not in got
    assert "__reserved__" not in got

    sub.close()
    pub.close()


def test_reactive_invalid_msg_from(broker):
    cli = rb.node(f"ws://:{PORT}/md_badfrom")
    with pytest.raises(rb.RembusError):
        cli.reactive(msg_from=-1)
    cli.reactive(msg_from=0)
    cli.close()
