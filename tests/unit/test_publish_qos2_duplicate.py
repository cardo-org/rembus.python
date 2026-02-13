"""Test pubsub QOS2 when duplicated messages are sent."""

import asyncio
import logging
import rembus
import rembus.protocol as rp

PAYLOAD = 1
RECEIVED = None


async def mytopic(data):
    """A simple pubsub handler that logs the received data."""
    global RECEIVED  # pylint: disable=global-statement
    logging.info("[mytopic]: %s", data)
    RECEIVED = PAYLOAD


async def test_publish(mocker, ws_mock):
    """Test the publish method with QoS 2."""
    responses = [
        {
            # identity
            "reply": lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            # subscribe
            "reply": lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            # publish
        },
        {
            # ack
            "discard": True
        },
        {
            # publish
        },
        {
            # ack
        },
    ]

    mocked_connect = mocker.patch(
        "websockets.connect", mocker.AsyncMock(return_value=ws_mock(responses))
    )

    rb = await rembus.component("foo")
    mocked_connect.assert_called_once()
    assert mocked_connect.call_args[0][0] == "ws://127.0.0.1:8000"

    assert rb.uid.id == "foo"

    await rb.subscribe(mytopic)

    # send two pubsub messages with the same msgid
    msgid = bytes([i for i in range(rp.MSGID_SZ)])
    topic = mytopic.__name__
    for i in range(3):
        rb.outreq[rp.from_bytes(msgid)] = rembus.core.FutureResponse(True)
        req = rp.encode([rp.TYPE_PUB | rembus.QOS2, msgid, topic, PAYLOAD])
        if rb.socket:
            await rb.socket.send(req)

    await asyncio.sleep(0.1)
    assert RECEIVED == PAYLOAD
    await rb.close()
