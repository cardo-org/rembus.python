import logging
import rembus
import websockets

async def myservice(data):
    logging.info(f'[myservice]: {data}')
    return data*2

async def test_rpc_method_exception(mocker, WebSocketMockFixture):
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

    try:
        await rb.rpc(myservice.__name__)
    except Exception as e:
        logging.info(e)
        assert isinstance(e, rembus.RembusError)
        assert e.status == rembus.METHOD_EXCEPTION
        assert e.message == "myservice() missing 1 required positional argument: 'data'"
    await rb.close()   