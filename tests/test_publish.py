import asyncio
import logging
import cbor2
import rembus
import websockets
from unittest.mock import patch

payload = 1

mytopic_received = None

async def myservice(data):
    logging.info(f'[myservice]: {data}')
    return data*2

async def mytopic(data):
    global mytopic_received
    logging.info(f'[mytopic]: {data}')
    mytopic_received = payload

async def test_publish(mocker, WebSocketMockFixture):
    global mytopic_received

    responses = [
        [rembus.TYPE_RESPONSE, rembus.OK, None], # identity
        [rembus.TYPE_RESPONSE, rembus.OK, None], # subscribe
        [rembus.TYPE_PUB],
        [rembus.TYPE_RESPONSE, rembus.OK, None], # unsubscribe
        [rembus.TYPE_PUB],
    ]
    mocker.patch(
        "websockets.connect",mocker.AsyncMock(return_value=WebSocketMockFixture(responses))
    )

    rb = await rembus.component('foo')
    websockets.connect.assert_called_once_with('ws://localhost:8000/foo', ssl=None)
    assert rb.component.name == 'foo'

    await rb.subscribe(mytopic)
    await rb.publish(mytopic.__name__, payload)
    await asyncio.sleep(0.1)
    assert mytopic_received == payload

    mytopic_received = None
    await rb.unsubscribe(mytopic)
    await rb.publish(mytopic.__name__, payload)

    await asyncio.sleep(0.1)
    assert mytopic_received == None
    await rb.close()
