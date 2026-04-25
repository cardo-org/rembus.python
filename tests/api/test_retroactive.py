import logging
import time
import rembus as rb


def retro_topic(ctx, node):
    logging.info("[%s] recv retro_topic", node)
    ctx[node.rid] += 1


def test_retroactive(server):
    cli = rb.node("cli")
    cli.publish("retro_topic")
    cli.close()
    time.sleep(1)

    ctx = {"sub": 0}
    sub = rb.node("sub")
    sub.inject(ctx)
    sub.subscribe(retro_topic, 900_000_000)  # 900 usec
    sub.reactive()
    time.sleep(1)
    sub.close()
    logging.info("[test_retroactive] ctx Now: %s", ctx)
    assert ctx["sub"] == 0

    sub = rb.node("sub")
    sub.inject(ctx)
    sub.subscribe(retro_topic, rb.LastReceived)
    sub.reactive()
    time.sleep(0.1)
    sub.close()
    logging.info("[test_retroactive] ctx LastReceived: %s", ctx)
    assert ctx["sub"] == 1
