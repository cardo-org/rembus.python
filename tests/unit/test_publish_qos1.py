"""Tests the loss of pubsub Ack message."""
import asyncio
import logging
import rembus
import rembus.protocol as rp

PAYLOAD = 1
RECEIVED = None


async def mytopic(data):
    """A simple pubsub handler that logs the received data."""
    global RECEIVED  # pylint: disable=global-statement
    logging.info('[mytopic]: %s', data)
    RECEIVED = PAYLOAD


async def test_publish(mocker, ws_mock):
    """Test the publish method of the rembus component with QoS 1."""
    global RECEIVED  # pylint: disable=global-statement

    responses = [
        {
            # identity
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            # subscribe
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            # publish
        },
        {
            # ack
            'discard': True
        },
        {
            # ack
            'discard': False
        },
        {
            # unsubscribe
        },
        {
            # publish
        }
    ]

    mocked_connect = mocker.patch(
        "websockets.connect", mocker.AsyncMock(
            return_value=ws_mock(responses))
    )

    rb = await rembus.component('foo')
    mocked_connect.assert_called_once()
    assert mocked_connect.call_args[0][0] == "ws://127.0.0.1:8000/foo"

    assert rb.uid.id == 'foo'

    await rb.subscribe(mytopic)
    await rb.publish(mytopic.__name__, PAYLOAD, qos=rembus.QOSLevel.QOS1)
    await asyncio.sleep(0.1)
    assert RECEIVED == PAYLOAD

    RECEIVED = None
    await rb.unsubscribe(mytopic)
    await rb.publish(mytopic.__name__, (PAYLOAD,))

    await asyncio.sleep(0.1)
    assert RECEIVED is None
    await rb.close()
