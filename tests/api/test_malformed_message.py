"""A malformed message closes the connection with close code 1008."""

import cbor2
import pytest
import websockets

import rembus
import rembus.protocol as rp

URL = "ws://localhost:8992/"


@pytest.mark.parametrize("payload", [
        b"",
        b"\xff\xff",
        "{bad",
        "[1]",
        cbor2.dumps([rp.TYPE_RPC, b"\x01" * 8, "m", None]),
    ])
async def test_malformed_closes_connection(payload):
    server = await rembus.component(port=8992)
    try:
        async with websockets.connect(URL) as ws:
            await ws.send(payload)
            with pytest.raises(websockets.ConnectionClosed):
                await ws.recv()
            assert ws.close_code == rp.WS_CLOSE_MALFORMED
    finally:
        await server.close()
