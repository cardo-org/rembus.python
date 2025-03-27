import functools
import threading
import time
from websockets.sync.client import connect
from rembus.common import *

# Global dictionary to store connected components
connected_components = {}

def add_component(name, component):
    """Add a component to the connected components dictionary."""
    connected_components[name] = component

def get_component(name):
    """Retrieve a component from the connected components dictionary."""
    return connected_components.get(name)

def remove_component(name):
    """Remove a component from the connected components dictionary."""
    if name in connected_components:
        del connected_components[name]

def connector(cmp):
    retries = 0
    while True:
        if cmp.ws:
            with cmp.isdone:
                cmp.isdone.wait()

        if cmp.isclosing():
            break

        time.sleep(4)
        logger.debug(f"{cmp.name}: reconnecting ...")
        cmp.connect()
        retries += 1

def component(name=None):
    if name in connected_components:
        return connected_components[name]
    else:
        cmp = Rembus(name)
        cmp.connect()
        add_component(name, cmp)
    
        conn = threading.Thread(
            target=connector, args=(cmp,),
            daemon=False)
        conn.start()
        return cmp

class Rembus:
    def __init__(self, name=None):
        self.name = name
        self.status = 'IDLE'
        self.ws = None
        self.receiver = None
        self.component = Component(name)

        # outstanding requests
        self.outreq = {}
        self.handler = {}

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()

    def evaluate(self, topic, data):
        """:meta private:"""
        if isinstance(data, list):
            output = self.handler[topic](*data)
        elif isinstance(data, bytes):
            args = list(data)
            output = self.handler[topic](*args)
        else:
            output = self.handler[topic](data)
        return output

    def parse_input(self, msg):
        """:meta private:"""
        type_byte, msgid = msg[0:2]

        type = type_byte & 0x3F
        flags = type_byte & 0xC0
        logger.debug(f"recv packet type {type}, flags:{flags}")
        if type == TYPE_PUB:
            data = tag2df(msg[2])
            try:
                self.evaluate(msgid, data)
            except Exception as e:
                logger.error(f"{e}")
            return
        elif type == TYPE_RPC:
            data = tag2df(msg[4])
            topic = msg[2]

            if not topic in self.handler:
                outmsg = [TYPE_RESPONSE, msgid, METHOD_NOT_FOUND, topic]
            else:
                status = OK
                try:
                    output = self.evaluate(topic, data)
                except Exception as e:
                    status = METHOD_EXCEPTION
                    output = f"{e}"
                    logger.info(f"exception: {e}")

                outmsg = [TYPE_RESPONSE, msgid, status, df2tag(output)]
                logger.debug(msg_str('out', outmsg))

            self.ws.send(cbor2.dumps(outmsg))
            return

        condition = self.outreq.pop(msgid, None)
        if condition == None:
            logger.warning(f"recv unknown msg id {tohex(msgid)}")
            return
        with condition:
            if type == TYPE_RESPONSE:
                sts = msg[2]
                if sts == OK:
                    self.outreq[msgid] = tag2df(msg[3])
                elif sts == CHALLENGE:
                    self.outreq[msgid] = msg[3]
                else:
                    self.outreq[msgid] = RembusError(sts, msg[3])

            condition.notify()

    def receive(self):
        """:meta private:"""
        try:
            while True:
                result = self.ws.recv()
                msg = cbor2.loads(result)
                logger.debug(msg_str('in', msg))
                self.parse_input(msg)
        except websockets.exceptions.ConnectionClosedError:
            logger.info("unexpected ws close connection")
        except Exception as e:
            logger.info(f"closing ({type(e)}): {e}")
        finally:
            self.close_connection()

            with self.isdone:
                self.isdone.notify()
            
            # notify all outstanding requests
            for reqid in self.outreq:
                cond = self.outreq[reqid]
                if isinstance(cond, threading.Condition):
                    with cond:
                        self.outreq[reqid] = RembusConnectionClosed()
                        cond.notify()

    def connect(self):
        self.isdone = threading.Condition()
        broker_url = self.component.connection_url()

        ssl_context = None
        if self.component.scheme == "wss":
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ca_crt = os.getenv("HTTP_CA_BUNDLE", "rembus-ca.crt")
            if os.path.isfile(ca_crt):
                ssl_context.load_verify_locations(ca_crt)
            else:
                logger.warning(f"CA file not found: {ca_crt}")

        try:
            self.ws = connect(broker_url, max_size=WS_FRAME_MAXSIZE, ssl_context=ssl_context)
            self.receiver = threading.Thread(
                target=self.receive, args=(), daemon=False)
            self.receiver.start()
            if self.component.name != None:
                try:
                    self.login()
                except Exception as e:
                    raise RembusError(IDENTIFICATION_ERROR)
            self.status = 'CONNECTED'
        except ConnectionRefusedError:
            with self.isdone:
                self.isdone.notify()
        return self

    def timeout(self, reqid):
        cond = self.outreq[reqid]
        if isinstance(cond, threading.Condition):
            with cond:
                self.outreq[reqid] = RembusTimeout()
                cond.notify()

    def send_wait(self, builder):
        """:meta private:"""
        reqid = id()
        condition = threading.Condition()
        self.outreq[reqid] = condition
        self.ws.send(builder(reqid))

        timer = threading.Timer(
            request_timeout(), lambda reqid: self.timeout(reqid), args=(reqid,)
        )
        timer.start()

        with condition:
            condition.wait()
        timer.cancel()
        result = self.outreq.pop(reqid)
        if isinstance(result, RembusException):
            raise result
        
        return result

    def login(self):
        """:meta private:"""
        challenge = self.send_wait(
            lambda id: encode([TYPE_IDENTITY, id, self.component.name])
        )
        if challenge and isinstance(challenge, bytes):
            logger.debug(f"challenge: {challenge}")
            plain = [bytes(challenge), self.component.name]
            message = cbor2.dumps(plain)
            self.privatekey = load_private_key(self.component.name)
            signature = self.privatekey.sign(
                message, padding.PKCS1v15(), hashes.SHA256()
            )
            self.send_wait(
                lambda id: encode(
                    [TYPE_ATTESTATION, id, self.component.name, signature])
            )
        else:
            logger.debug(f"cid {self.component.name}: free mode access")

    def publish(self, topic, *args):
        data = df2tag(args)
        self.ws.send(encode([TYPE_PUB, topic, data]))

    def broker_setting(self, command, args={}):
        data = {COMMAND: command} | args
        return self.send_wait(
            lambda id: encode([TYPE_ADMIN, id, BROKER_CONFIG, data])
        )

    def setting(self, topic, command, args={}):
        data = {COMMAND: command} | args
        return self.send_wait(lambda id: encode([TYPE_ADMIN, id, topic, data]))

    def rpc(self, topic, *args):
        data = df2tag(args)
        return self.send_wait(
            lambda id: encode([TYPE_RPC, id, topic, None, data]))

    def direct(self, target, topic, *args):
        data = df2tag(args)
        return self.send_wait(
            lambda
            id: encode(
                [TYPE_RPC, id, topic, target, data]))

    def reactive(self):
        self.broker_setting("reactive", {"status": True})
        return self

    def unreactive(self):
        self.broker_setting("reactive", {"status": False})
        return self

    def subscribe(self, fn, retroactive=False):
        topic = fn.__name__
        self.setting(topic, ADD_INTEREST, {"retroactive": retroactive})
        self.handler[topic] = fn
        return self

    def unsubscribe(self, fn):
        if isinstance(fn, str):
            topic = fn
        else:
            topic = fn.__name__

        self.setting(topic, REMOVE_INTEREST)
        self.handler.pop(topic, None)
        return self

    def expose(self, fn):
        topic = fn.__name__
        self.handler[topic] = fn
        self.setting(topic, ADD_IMPL)

    def unexpose(self, fn):
        if isinstance(fn, str):
            topic = fn
        else:
            topic = fn.__name__

        self.handler.pop(topic, None)
        self.setting(topic, REMOVE_IMPL)

    def close_connection(self):
        if not self.isclosing():
            self.status = 'DISCONNECTED'

        if self.ws:
            self.ws.close()
            self.ws = None

    def close(self):
        self.status = 'CLOSED'
        remove_component(self.name)
        self.close_connection()

    def isclosing(self):
        return self.status == 'CLOSED'
    
    def isopen(self):
        return self.status == 'CONNECTED'

    def forever(self):
        self.reactive()
        self.receiver.join()
