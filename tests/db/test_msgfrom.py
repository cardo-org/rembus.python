import asyncio
import logging
import rembus as rb


def foo(d):
    logging.info("foo recv: %s", d)


def sum(x, y):
    logging.info("sum recv: x=%s, y=%s", x, y)


def signal():
    logging.info("signal recv")


async def test_msg_from():
    bro = await rb.component()
    cli = await rb.component("publisher")

    await cli.publish("foo", {"name": "bar"})
    await cli.publish("sum", 1, 2)
    await cli.publish("signal")

    await asyncio.sleep(3)

    sub = await rb.component("subscriber")

    await sub.subscribe(foo, msgfrom=rb.LastReceived)
    await sub.subscribe(sum, msgfrom=rb.LastReceived)
    await sub.subscribe(signal, msgfrom=rb.LastReceived)

    await sub.reactive()
    await asyncio.sleep(1)

    await cli.close()
    await sub.close()
    await bro.close()
