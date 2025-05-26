import asyncio
import cbor2
import json
import os
import pytest
import rembus
import rembus.protocol as rp
import shutil
import logging
import websockets

def pytest_configure(config):
    logging.getLogger().setLevel(logging.DEBUG)

    logging.getLogger('rembus').setLevel(logging.DEBUG)

    logging.getLogger('websockets').setLevel(logging.INFO)

@pytest.fixture(scope="session", autouse=True)
def setup_before(request):
    """
    Fixture to set up resources before the entire test session starts.
    """
    os.environ["REMBUS_DIR"] = os.path.join("tmp", "rembus")

    rembus_dir = rembus.rembus_dir()
    if os.path.exists(rembus_dir):
        shutil.rmtree(rembus_dir)

    broker_dir = os.path.join(rembus.rembus_dir(), rembus.DEFAULT_BROKER)
    os.makedirs(broker_dir)

    # Setup tenant settings
    fn = os.path.join(broker_dir, rembus.TENANTS_FILE)
    with open(fn, 'w') as f:
        f.write(json.dumps({".": "11223344"}))

    yield


class WebSocketMock:
    def __init__(self, responses):
        self.count = 0
        self.responses = responses
        self.queue = asyncio.Queue()
        self.state = websockets.State.OPEN

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def build_response(self, msg):
        logging.debug(f'[mock_build_response]: count [{self.count}]: {msg}')
        if not self.responses:
            # by default return a STS_OK response
            return cbor2.dumps([rp.TYPE_RESPONSE, msg[1], rp.STS_OK, None])
        
        step = self.responses[self.count]
        self.count += 1
        if 'reply' in step:
            response_msg = step['reply'](msg)
            return cbor2.dumps(response_msg)
        else:
            return cbor2.dumps(msg)

    async def send(self, pkt):
        logging.debug(f'[mock_send]: count [{self.count}]: {pkt}')
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
            logging.debug(f'[mock_send]: {msg}')
            await self.queue.put(msg)
        else:
            self.count += 1
        
    async def close(self):
        pass

    async def recv(self):
        pkt = await self.queue.get()
        # logging.debug(f'[wsmock] recv: {pkt}')
        if isinstance(pkt, str):
            msg = pkt
        elif pkt[0] in [rp.TYPE_RESPONSE,
                      rp.TYPE_ACK]:
            # the message was already processed by rembus handler
            msg = cbor2.dumps(pkt)
        else:
            # build the response for identity, setting, expose, subscribe 
            msg = self.build_response(pkt)
        #logging.debug(f'[wsmock] response: {rembus.tohex(msg)}')
        self.queue.task_done()
        return msg

@pytest.fixture
def WebSocketMockFixture():
    return WebSocketMock
