import asyncio
import logging
import cbor2
import rembus
import rembus.protocol as rp
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
        {
            #identity
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            #subscribe 
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None] 
        },
        {
            #publish
        }, 
        {
            #ack
        }, 
        {
            #ack2
        }, 
    ]

    mocked_connect = mocker.patch(
        "websockets.connect",mocker.AsyncMock(return_value=WebSocketMockFixture(responses))
    )

    rb = await rembus.component('foo')
    mocked_connect.assert_called_once()
    assert mocked_connect.call_args[0][0] == "ws://127.0.0.1:8000/foo"

    assert rb.uid.id == 'foo'

    await rb.subscribe(mytopic)
    
    # send a response message with an unknown msgid
    msgid = bytes([i for i in range(16)])
    topic = mytopic.__name__
    req = rembus.twin.encode(
                [rp.TYPE_RESPONSE, msgid, topic, payload]
            )
    await rb.socket.send(req)
        
    await rb.close()
