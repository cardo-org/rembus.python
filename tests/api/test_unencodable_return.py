"""A return value that cannot be encoded is answered with status 40."""

import pytest

import rembus
import rembus.protocol as rp


async def test_unencodable_return_value():
    server = await rembus.component(port=8993)
    client = None
    try:

        async def bad():
            return object()

        async def good():
            return [1]

        await server.expose(bad)
        await server.expose(good)
        client = await rembus.component("ws://127.0.0.1:8993/unenc_cli")

        with pytest.raises(rp.RembusError) as exc:
            await client.rpc("bad")
        assert exc.value.status == rp.STS_METHOD_EXCEPTION
        assert await client.rpc("good") == [1]
    finally:
        if client:
            await client.close()
        await server.close()
