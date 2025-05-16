import asyncio
import cbor2
import pytest
import rembus
import rembus.protocol as rp
import logging
import websockets

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
