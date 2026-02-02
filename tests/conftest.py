"""Test configuration and setup for rembus tests."""

import asyncio
import logging
import os
import time
import shutil
import websockets
import cbor2
import pytest
import rembus
import rembus.protocol as rp


@pytest.fixture(scope="module")
def server(port=8000):
    """
    Fixture to start and yield a rembus server, then shut it down.
    The server will be available to tests that request this fixture.
    """
    rb = rembus.node(port=port)
    rb.register_shutdown()

    time.sleep(1)  # Give the server a moment to start
    logging.info("starting test rembus server")
    yield rb
    logging.info("shutting down test rembus server")
    rb.close()


def pytest_configure(config):  # pylint: disable=unused-argument
    """
    Configure pytest settings before any tests are run.
    """
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger("rembus").setLevel(logging.DEBUG)
    logging.getLogger("websockets").setLevel(logging.INFO)


@pytest.fixture(scope="session", autouse=True)
def setup_before(request):  # pylint: disable=unused-argument
    """
    Fixture to set up resources before the entire test session starts.
    """

    # Return the default rembus directory if REMBUS_DIR is not set
    rembus.rembus_dir()

    os.environ["REMBUS_DIR"] = os.path.join("tmp", "rembus")

    rembus_dir = rembus.rembus_dir()
    if os.path.exists(rembus_dir):
        shutil.rmtree(rembus_dir)

    broker_dir = os.path.join(rembus.rembus_dir(), rembus.DEFAULT_BROKER)

    rembus.db.reset_db(rembus.DEFAULT_BROKER)
    os.makedirs(broker_dir)

    yield


class WebSocketMock:
    """A mock WebSocket for testing purposes."""

    def __init__(self, responses):
        self.count = 0
        self.responses = responses
        self.queue = asyncio.Queue()
        self.state = websockets.State.OPEN

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def _build_response(self, msg):
        if not self.responses:
            # by default return a STS_OK response
            return cbor2.dumps([rp.TYPE_RESPONSE, msg[1], rp.STS_OK, None])

        step = self.responses[self.count]
        self.count += 1
        if "reply" in step:
            response_msg = step["reply"](msg)
            return cbor2.dumps(response_msg)
        else:
            return cbor2.dumps(msg)

    async def send(self, pkt):
        """Send a message through the mock WebSocket."""
        if self.count >= len(self.responses):
            step = {}
        else:
            step = self.responses[self.count]

        if not dict.get(step, "discard", False):
            # just to start wait before notifying
            await asyncio.sleep(0.1)
            if isinstance(pkt, bytes):
                msg = cbor2.loads(pkt)
            else:
                msg = pkt
            # logging.debug("[mock_send]: %s", msg)
            await self.queue.put(msg)
        else:
            self.count += 1

    async def close(self):
        """Close the mock WebSocket."""

    async def recv(self):
        """Receive a message from the mock WebSocket."""
        pkt = await self.queue.get()
        # logging.debug(f'[wsmock] recv: {pkt}')
        if isinstance(pkt, str):
            msg = pkt
        elif pkt[0] in [rp.TYPE_RESPONSE, rp.TYPE_ACK]:
            # the message was already processed by rembus handler
            msg = cbor2.dumps(pkt)
        else:
            # build the response for identity, setting, expose, subscribe
            msg = self._build_response(pkt)
        # logging.debug(f'[wsmock] response: {rembus.tohex(msg)}')
        self.queue.task_done()
        return msg


@pytest.fixture
def ws_mock():
    """Fixture to create a WebSocketMock instance for testing."""
    return WebSocketMock
