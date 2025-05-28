import logging
import rembus
import rembus.protocol as rp

payload = 1


async def myservice(data):
    logging.info('[myservice]: %s', data)
    return data*2


async def test_rpc_method_unkown(mocker, WebSocketMockFixture):
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

    invalid_method = 'invalid_method'
    try:
        await rb.rpc(invalid_method, payload)
    except rp.RembusError as e:
        assert isinstance(e, rp.RembusError)
        assert e.status == rp.STS_METHOD_NOT_FOUND
        assert e.message == invalid_method
    await rb.close()
