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

async def test_publish_unknow_topic(mocker, WebSocketMockFixture):
    topic = "unknown_topic"

    responses = [
        [rembus.TYPE_RESPONSE, rembus.OK, None], # identity
        [rembus.TYPE_RESPONSE, rembus.OK, None], # subscribe
        [rembus.TYPE_PUB]
    ]

    mocker.patch(
        "websockets.connect",mocker.AsyncMock(return_value=WebSocketMockFixture(responses))
    )
    
    rb = await rembus.component('foo')
    websockets.connect.assert_called_once_with('ws://localhost:8000/foo', ssl=None)
    logging.info(f'name: {rb.component.name}')
    
    await rb.subscribe(mytopic)
    await rb.publish(topic, payload)
    await rb.close()
