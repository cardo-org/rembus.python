import logging
import time
import rembus as rb

def mytopic(ctx, node):
    logging.info("[%s] recv mytopic", node)
    ctx[node.rid] += 1

def test_retroactive(server):
    cli = rb.node("cli")
    cli.publish("mytopic")
    cli.close()
    time.sleep(1)

    ctx = {"sub": 0}
    sub = rb.node("sub")
    sub.inject(ctx)
    sub.subscribe(mytopic, 900_000_000) # 900 usec
    sub.reactive()
    time.sleep(1)
    sub.close()
    logging.info("[test_retroactive] ctx Now: %s", ctx)
    assert ctx["sub"] == 0

    sub = rb.node("sub")
    sub.inject(ctx)
    sub.subscribe(mytopic, rb.LastReceived)
    sub.reactive()
    time.sleep(0.1)
    sub.close()
    logging.info("[test_retroactive] ctx LastReceived: %s", ctx)
    assert ctx["sub"] == 1