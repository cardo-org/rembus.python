"""Test the overwrite_connection setting."""

import pytest
import rembus
import rembus.protocol as rp

URL = "ws://:8991/dup"


async def test_already_connected_rejected():
    """By default a second connection with the same cid is rejected."""
    server = await rembus.component(port=8991)
    first = second = None
    try:
        assert server.router.config.overwrite_connection is False
        first = await rembus.component(URL, name="dup1")
        with pytest.raises(rp.RembusError):
            second = await rembus.component(URL, name="dup2")
        assert await first.rpc("rid") == rembus.settings.DEFAULT_BROKER
    finally:
        for node in (second, first, server):
            if node:
                await node.close()


async def test_overwrite_connection():
    """With overwrite_connection the prior twin is replaced."""
    server = await rembus.component(port=8991)
    first = second = None
    try:
        server.router.config.overwrite_connection = True
        first = await rembus.component(URL, name="dup1")
        second = await rembus.component(URL, name="dup2")
        assert await second.rpc("rid") == rembus.settings.DEFAULT_BROKER
    finally:
        for node in (second, first, server):
            if node:
                await node.close()
