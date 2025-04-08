import logging
import rembus
import websockets

payload = 1

async def myservice(data):
    logging.info(f'[myservice]: {data}')
    return data*2

async def test_rpc_method_unkown(mocker, WebSocketMockFixture):
    responses = [
        [rembus.TYPE_RESPONSE, rembus.OK, None], # identity
        [rembus.TYPE_RESPONSE, rembus.OK, None], # expose
        [rembus.TYPE_RPC], # rpc request
    ]

    mocker.patch(
        "websockets.connect",mocker.AsyncMock(return_value=WebSocketMockFixture(responses))
    )
    rb = await rembus.component('bar')
    websockets.connect.assert_called_once_with('ws://localhost:8000/bar', ssl=None)
    await rb.expose(myservice)

    invalid_method = 'invalid_method'
    try:
        response = await rb.rpc(invalid_method, payload)
    except Exception as e:
        logging.info(e.message)
        assert isinstance(e, rembus.RembusError)
        assert e.status == rembus.METHOD_NOT_FOUND
        assert e.message == invalid_method
    await rb.close()
