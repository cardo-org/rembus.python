import asyncio
import cbor2
import pytest
import rembus
import rembus.protocol
import logging

class WebSocketMock:
    def __init__(self, responses):
        self.count = 0
        self.responses = responses
        self.queue = asyncio.Queue()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def build_response(self, msg):
        step = self.responses[self.count]
        self.count += 1
        if 'reply' in step:
            response_msg = step['reply'](msg)
            return cbor2.dumps(response_msg)
        else:
            return cbor2.dumps(msg)

    async def send(self, pkt):
        if self.count >= len(self.responses):
            step = {}
        else:
            step = self.responses[self.count]

        if not dict.get(step, "discard", False):
            # just to start wait before notifying
            await asyncio.sleep(0.1)
            msg = cbor2.loads(pkt)
            #logging.debug(f'ws mock send: {msg}')
            await self.queue.put(msg)
        else:
            self.count += 1
        

    async def close(self):
        pass

    async def recv(self):
        pkt = await self.queue.get()
        # logging.debug(f'[wsmock] recv: {pkt}')
        if pkt[0] in [rembus.protocol.TYPE_RESPONSE,
                      rembus.protocol.TYPE_ACK]:
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