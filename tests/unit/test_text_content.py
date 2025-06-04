"""Test the case of receiving text messages instead of binary data."""
import asyncio
import rembus
import rembus.protocol as rp


async def test_send_text(mocker, ws_mock):
    """Send a text message instead of binary data."""
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
        {
            # ack
        },
        {
            # ack2
        },
    ]

    mocked_connect = mocker.patch(
        "websockets.connect", mocker.AsyncMock(
            return_value=ws_mock(responses))
    )

    rb = await rembus.component('foo')
    mocked_connect.assert_called_once()
    assert mocked_connect.call_args[0][0] == "ws://127.0.0.1:8000/foo"
    assert rb.uid.id == 'foo'
    # send a response message with an unknown msgid
    if rb.socket:
        await rb.socket.send("ola mondo")
    await asyncio.sleep(0.1)
    await rb.close()
