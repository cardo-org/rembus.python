import logging
import rembus
import rembus.protocol as rp

payload = 1


async def myservice(data):
    logging.info(f'[myservice]: {data}')
    return data*2


async def test_unexpose(mocker, WebSocketMockFixture):
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
            # unexpose
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

    response = await rb.rpc(myservice.__name__, payload)
    assert response == payload*2

    await rb.unexpose(myservice)
    try:
        await rb.rpc(myservice.__name__, payload)
    except rp.RembusError as e:
        assert e.status == rp.STS_METHOD_NOT_FOUND

    await rb.close()
