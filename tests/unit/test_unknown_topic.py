"""Test that publishing to an unknown topic does not raise an error."""

import rembus
import rembus.protocol as rp


async def test_publish_unknow_topic(mocker, ws_mock):
    """Send a message to an unknown topic without raising an error."""
    topic = "unknown_topic"

    responses = [
        {
            # identity
            "reply": lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            # subscribe
            "reply": lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            # publish
        },
    ]

    mocked_connect = mocker.patch(
        "websockets.connect", mocker.AsyncMock(return_value=ws_mock(responses))
    )

    rb = await rembus.component("foo")

    mocked_connect.assert_called_once()
    assert mocked_connect.call_args[0][0] == "ws://127.0.0.1:8000"
    await rb.publish(topic, "payload")
    await rb.close()
