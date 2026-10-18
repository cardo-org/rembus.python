"""Test that the WebSocket size limit is enforced per frame, not per message."""

import rembus
import rembus.protocol as rp
from rembus.settings import MAX_WS_FRAME


async def test_connect_max_size_is_per_frame(mocker, ws_mock):
    """The client passes (no message limit, per-frame limit) to websockets."""
    responses = [
        {
            # identity
            "reply": lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
    ]
    mocked_connect = mocker.patch(
        "websockets.connect", mocker.AsyncMock(return_value=ws_mock(responses))
    )

    rb = await rembus.component("foo")
    mocked_connect.assert_called_once()
    assert mocked_connect.call_args.kwargs["max_size"] == (None, MAX_WS_FRAME)
    await rb.close()
