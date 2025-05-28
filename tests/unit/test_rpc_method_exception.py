import logging
import rembus
import rembus.protocol as rp


async def myservice(data):
    logging.info('[myservice]: %s', data)
    return data*2


async def test_rpc_method_exception(mocker, WebSocketMockFixture):
    responses = [
        [rp.TYPE_RESPONSE, rp.STS_OK, None],  # identity
        [rp.TYPE_RESPONSE, rp.STS_OK, None],  # expose
        [rp.TYPE_RPC],  # rpc request
    ]

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
    ]

    mocked_connect = mocker.patch(
        "websockets.connect", mocker.AsyncMock(
            return_value=WebSocketMockFixture(responses))
    )
    rb = await rembus.component('bar')

    mocked_connect.assert_called_once()
    assert mocked_connect.call_args[0][0] == "ws://127.0.0.1:8000/bar"

    await rb.expose(myservice)

    try:
        await rb.rpc(myservice.__name__)
    except rp.RembusError as e:
        logging.info(e)
        assert isinstance(e, rp.RembusError)
        assert e.status == rp.STS_METHOD_EXCEPTION
        assert e.message == "myservice() missing 1 required positional argument: 'data'"
    await rb.close()
