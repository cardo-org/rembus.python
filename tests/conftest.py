import asyncio
import cbor2
import pytest
import rembus

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
        type_reply = self.responses[self.count][0]
        if type_reply in [rembus.TYPE_RPC, rembus.TYPE_PUB]:
            # pass to application layer
            response_msg = msg
        else:
            msgid = msg[1]
            topic = msg[2]
            #logging.debug(f'[{topic}]: building response')
            sts = self.responses[self.count][1]
            data = self.responses[self.count][2]
            response_msg = [type_reply, msgid, sts, data]
        
        self.count += 1
        return cbor2.dumps(response_msg)
       
    async def send(self, pkt):
        # just to start wait before notifying
        await asyncio.sleep(0.1)
        msg = cbor2.loads(pkt)
        type = msg[0]
        # logging.debug(f'[wsmock] send: type {type} - send [{msg}]')
        await self.queue.put(msg)

    async def close(self):
        pass

    async def recv(self):
        pkt = await self.queue.get()
        # logging.debug(f'[wsmock] recv: {pkt}')
        if pkt[0] == rembus.TYPE_RESPONSE:
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