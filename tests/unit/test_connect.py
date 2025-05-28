import rembus


async def test_connect(mocker, WebSocketMockFixture):
    responses = [

    ]

    mocked_connect = mocker.patch(
        "websockets.connect", mocker.AsyncMock(
            return_value=WebSocketMockFixture(responses))
    )

    rb = await rembus.component()

    assert rb.isopen()

    mocked_connect.assert_called_once()
    assert mocked_connect.call_args[0][0].startswith("ws://127.0.0.1:8000")
    await rb.close()
