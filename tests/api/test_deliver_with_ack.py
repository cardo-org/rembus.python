"""Tests for Twin.deliver_with_ack: the broker->subscriber QOS1/QOS2
fan-out hop performed by Router._broadcast.
"""

import asyncio
import logging
import pytest
import rembus
import rembus.protocol as rp

RECEIVED = None


async def mytopic(data):
    """A simple pubsub handler that records the received data."""
    global RECEIVED  # pylint: disable=global-statement
    logging.info("[mytopic]: %s", data)
    RECEIVED = data


async def start_server(port):
    """Start a rembus server on the given port."""
    server = await rembus.component(port=port)
    await asyncio.sleep(1)
    return server


@pytest.mark.asyncio
@pytest.mark.parametrize("qos", [rp.QOS1, rp.QOS2])
async def test_deliver_with_ack_success(qos):
    """A reactive subscriber receiving a QOS1/QOS2 message completes the
    Ack/Ack2 handshake on the first attempt (deliver_with_ack happy path).
    """
    global RECEIVED  # pylint: disable=global-statement
    RECEIVED = None

    port = 8100 + qos
    server = await start_server(port)

    sub = await rembus.component(f"ws://:{port}/sub.net")
    await sub.subscribe(mytopic)
    await sub.reactive()

    pub = await rembus.component(f"ws://:{port}/pub.net")
    await pub.publish("mytopic", "hello", qos=qos)

    await asyncio.sleep(0.2)
    assert RECEIVED == "hello"

    await pub.close()
    await sub.close()
    await server.close()


@pytest.mark.asyncio
async def test_deliver_with_ack_retry_then_success():
    """If the first delivery attempt is lost, deliver_with_ack retries
    and eventually succeeds once the Ack handshake completes.
    """
    global RECEIVED  # pylint: disable=global-statement
    RECEIVED = None

    port = 8103
    server = await start_server(port)
    server.router.config.ack_timeout = 0.2

    sub = await rembus.component(f"ws://:{port}/sub.net")
    await sub.subscribe(mytopic)
    await sub.reactive()

    sub_twin = server.router.subscribers["mytopic"][0]
    original_send = sub_twin.send
    calls = {"count": 0}

    async def flaky_send(msg):
        calls["count"] += 1
        if calls["count"] == 1:
            # Simulate the first delivery attempt being lost: don't
            # actually send it, so no Ack comes back in time.
            return None
        return await original_send(msg)

    sub_twin.send = flaky_send

    pub = await rembus.component(f"ws://:{port}/pub.net")
    await pub.publish("mytopic", "retry", qos=rp.QOS1)

    await asyncio.sleep(0.5)
    assert RECEIVED == "retry"
    assert calls["count"] >= 2

    await pub.close()
    await sub.close()
    await server.close()


@pytest.mark.asyncio
async def test_deliver_with_ack_exhausts_retries(caplog):
    """When every delivery attempt is lost and retries are exhausted,
    deliver_with_ack raises RembusTimeout, which Router._broadcast
    catches (via asyncio.gather(return_exceptions=True)) and logs,
    without the message ever reaching the subscriber.
    """
    global RECEIVED  # pylint: disable=global-statement
    RECEIVED = None

    port = 8104
    server = await start_server(port)
    server.router.config.ack_timeout = 0.2
    server.router.config.send_retries = 1

    sub = await rembus.component(f"ws://:{port}/sub.net")
    await sub.subscribe(mytopic)
    await sub.reactive()

    sub_twin = server.router.subscribers["mytopic"][0]

    async def dropped_send(msg):
        # Every delivery attempt is lost: never actually send it.
        return None

    sub_twin.send = dropped_send

    pub = await rembus.component(f"ws://:{port}/pub.net")
    with caplog.at_level(logging.WARNING):
        await pub.publish("mytopic", "lost", qos=rp.QOS1)
        await asyncio.sleep(1.0)

    assert RECEIVED is None
    assert any(
        "failed to deliver message" in rec.message for rec in caplog.records
    )

    await pub.close()
    await sub.close()
    await server.close()
