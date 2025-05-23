import logging
import rembus
import rembus.protocol as rp
import websockets

payload = 1

async def myservice(data):
    logging.info(f'[myservice]: {data}')
    return data*2

async def test_rpc(mocker, WebSocketMockFixture):
    responses = [
        {
            # identity
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            # expose 
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None] 
        },
        {
            # rpc
        },
        {
            # response from exposed method
        }
    ]

    mocked_connect = mocker.patch(
        "websockets.connect",mocker.AsyncMock(return_value=WebSocketMockFixture(responses))
    )

    rb = await rembus.component('bar')
    mocked_connect.assert_called_once()
    assert mocked_connect.call_args[0][0] == "ws://127.0.0.1:8000/bar"
    await rb.expose(myservice)

    response = await rb.rpc(myservice.__name__, payload)
    logging.info(f'response: {response}')
    assert response == payload*2

    await rb.close()
