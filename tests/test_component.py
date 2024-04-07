import asyncio
import logging
import cbor2
import rembus
import websockets
from unittest.mock import patch

payload = 1

mytopic_received = None

async def myservice(data):
    logging.info(f'[myservice]: {data}')
    return data*2

async def mytopic(data):
    global mytopic_received
    logging.info(f'[mytopic]: {data}')
    mytopic_received = payload

class WebSocketMock:
    def __init__(self, responses):
        self.count = 0
        self.responses = responses
        self.queue = asyncio.Queue()

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

def ws_mock(mocker, responses):
    return mocker.AsyncMock(return_value=WebSocketMock(responses))


async def test_publish_unknow_topic(mocker):
    topic = "unknown_topic"

    responses = [
        [rembus.TYPE_RESPONSE, rembus.OK, None], # identity
        [rembus.TYPE_RESPONSE, rembus.OK, None], # subscribe
        [rembus.TYPE_PUB]
    ]
    websockets.connect = ws_mock(mocker, responses)

    rb = await rembus.component('foo')
    websockets.connect.assert_called_once_with('ws://localhost:8000/foo', ssl=None)
    logging.info(f'name: {rb.component.name}')
    
    await rb.subscribe(mytopic)
    await rb.publish(topic, payload)
    await rb.close()

async def test_publish(mocker):
    global mytopic_received

    responses = [
        [rembus.TYPE_RESPONSE, rembus.OK, None], # identity
        [rembus.TYPE_RESPONSE, rembus.OK, None], # subscribe
        [rembus.TYPE_PUB],
        [rembus.TYPE_RESPONSE, rembus.OK, None], # unsubscribe
        [rembus.TYPE_PUB],
    ]
    websockets.connect = ws_mock(mocker, responses)

    rb = await rembus.component('foo')
    websockets.connect.assert_called_once_with('ws://localhost:8000/foo', ssl=None)
    assert rb.component.name == 'foo'

    await rb.subscribe(mytopic)
    await rb.publish(mytopic.__name__, payload)
    await asyncio.sleep(0.1)
    assert mytopic_received == payload

    mytopic_received = None
    await rb.unsubscribe(mytopic)
    await rb.publish(mytopic.__name__, payload)

    await asyncio.sleep(0.1)
    assert mytopic_received == None
    await rb.close()

async def test_rpc(mocker):
    responses = [
        [rembus.TYPE_RESPONSE, rembus.OK, None], # identity
        [rembus.TYPE_RESPONSE, rembus.OK, None], # expose
        [rembus.TYPE_RPC], # rpc request
    ]

    websockets.connect = ws_mock(mocker, responses)
    rb = await rembus.component('bar')
    websockets.connect.assert_called_once_with('ws://localhost:8000/bar', ssl=None)
    await rb.expose(myservice)

    response = await rb.rpc(myservice.__name__, payload)
    logging.info(f'response: {response}')
    assert response == payload*2

    await rb.close()

async def test_unexpose(mocker):
    responses = [
        [rembus.TYPE_RESPONSE, rembus.OK, None], # identity
        [rembus.TYPE_RESPONSE, rembus.OK, None], # expose
        [rembus.TYPE_RPC], # rpc request
        [rembus.TYPE_RESPONSE, rembus.OK, None], # unexpose
        [rembus.TYPE_RPC], # rpc request
    ]

    websockets.connect = ws_mock(mocker, responses)
    rb = await rembus.component('bar')
    websockets.connect.assert_called_once_with('ws://localhost:8000/bar', ssl=None)
    await rb.expose(myservice)

    response = await rb.rpc(myservice.__name__, payload)
    logging.info(f'response: {response}')
    assert response == payload*2

    await rb.unexpose(myservice)
    try:
        await rb.rpc(myservice.__name__, payload)
    except Exception as e:
        logging.info(f'unexpose: {e}')

    await rb.close()

async def test_rpc_method_unkown(mocker):
    responses = [
        [rembus.TYPE_RESPONSE, rembus.OK, None], # identity
        [rembus.TYPE_RESPONSE, rembus.OK, None], # expose
        [rembus.TYPE_RPC], # rpc request
    ]

    websockets.connect = ws_mock(mocker, responses)
    rb = await rembus.component('bar')
    websockets.connect.assert_called_once_with('ws://localhost:8000/bar', ssl=None)
    await rb.expose(myservice)

    invalid_method = 'invalid_method'
    try:
        response = await rb.rpc(invalid_method, payload)
    except Exception as e:
        logging.info(e.message)
        assert isinstance(e, rembus.RembusError)
        assert e.status == rembus.METHOD_NOT_FOUND
        assert e.message == invalid_method
    await rb.close()

async def test_rpc_method_exception(mocker):
    responses = [
        [rembus.TYPE_RESPONSE, rembus.OK, None], # identity
        [rembus.TYPE_RESPONSE, rembus.OK, None], # expose
        [rembus.TYPE_RPC], # rpc request
    ]

    websockets.connect = ws_mock(mocker, responses)
    rb = await rembus.component('bar')
    websockets.connect.assert_called_once_with('ws://localhost:8000/bar', ssl=None)
    await rb.expose(myservice)

    try:
        await rb.rpc(myservice.__name__)
    except Exception as e:
        logging.info(e)
        assert isinstance(e, rembus.RembusError)
        assert e.status == rembus.METHOD_EXCEPTION
        assert e.message == "myservice() missing 1 required positional argument: 'data'"
    await rb.close()   