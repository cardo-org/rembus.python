import asyncio
import logging
import cbor2
import rembus
import rembus.protocol as rp
import websockets
from unittest.mock import patch

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
    # send a response message with an unknown msgid
    msgid = bytes([i for i in range(16)])
    req = rembus.core.encode(
                [rp.TYPE_RESPONSE, msgid, 'topic', 'payload']
            )
    await rb.socket.send(req)
    await asyncio.sleep(0.1)
    await rb.close()
