import rembus
import rembus.protocol as rp


async def test_publish_unknow_topic(mocker, WebSocketMockFixture):
    topic = "unknown_topic"

    responses = [
        {
            # identity
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            # subscribe
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            # publish
        },
    ]

    mocked_connect = mocker.patch(
        "websockets.connect", mocker.AsyncMock(
            return_value=WebSocketMockFixture(responses))
    )

    rb = await rembus.component('foo')

    mocked_connect.assert_called_once()
    assert mocked_connect.call_args[0][0] == "ws://127.0.0.1:8000/foo"
    await rb.publish(topic, 'payload')
    await rb.close()
