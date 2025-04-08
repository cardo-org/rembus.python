import logging
import rembus
import websockets

payload = 1

async def myservice(data):
    logging.info(f'[myservice]: {data}')
    return data*2

async def test_unexpose(mocker, WebSocketMockFixture):
    responses = [
        [rembus.TYPE_RESPONSE, rembus.OK, None], # identity
        [rembus.TYPE_RESPONSE, rembus.OK, None], # expose
        [rembus.TYPE_RPC], # rpc request
        [rembus.TYPE_RESPONSE, rembus.OK, None], # unexpose
        [rembus.TYPE_RPC], # rpc request
    ]
    mocker.patch(
        "websockets.connect",mocker.AsyncMock(return_value=WebSocketMockFixture(responses))
    )

    rb = await rembus.component('bar')
    websockets.connect.assert_called_once_with('ws://localhost:8000/bar', ssl=None)
    await rb.expose(myservice)

    response = await rb.rpc(myservice.__name__, payload)
    logging.info(f'response: {response}')
    assert response == payload*2

    await rb.unexpose(myservice)
    try:
        await rb.rpc(myservice.__name__, payload)
    except Exception as e:
        logging.info(f'unexpose: {e}')

    await rb.close()
