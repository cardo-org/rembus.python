import logging
import rembus
import rembus.protocol as rp
import websockets

payload = 1

async def myservice(data):
    logging.info(f'[myservice]: {data}')
    return data*2

async def test_connect(mocker, WebSocketMockFixture):
    responses = [
        
    ]

    mocked_connect = mocker.patch(
        "websockets.connect",mocker.AsyncMock(return_value=WebSocketMockFixture(responses))
    )

    rb = await rembus.component("ws:")

    assert rb.isopen()

    mocked_connect.assert_called_once()
    assert mocked_connect.call_args[0][0].startswith("ws://127.0.0.1:8000")
#    await rb.expose(myservice)
#
#    response = await rb.rpc(myservice.__name__, payload)
#    logging.info(f'response: {response}')
#    assert response == payload*2

    await rb.close()
