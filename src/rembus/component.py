import asyncio
import atexit
from async_timeout import timeout
from rembus.common import *
import threading

logger = logging.getLogger("rembus")

background_tasks = set()

# This should live at module level if not already defined
_loop_runner = None

def get_loop_runner():
    global _loop_runner
    if _loop_runner is None:
        _loop_runner = AsyncLoopRunner()
    return _loop_runner

class AsyncLoopRunner:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._start_loop, daemon=True)
        self._thread.start()
        atexit.register(self.shutdown)

    def _start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run(self, coro):
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result()
    
    def shutdown(self):
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            self._thread.join()


async def component_task(cmp):
    while True:
        msg = await cmp.inbox.get()
        logger.debug(f"component task: {msg}")
        if msg == "shutdown":
            break

        logger.debug(f"{cmp.name}: reconnecting ...")
        try:
            await cmp.connect()
            await cmp.reactive()
        except Exception as e:
            logger.error(f"{cmp.name} connect: {e}")
            await asyncio.sleep(2)
            cmp.inbox.put_nowait("reconnect")

async def component(name=None):
    if name in connected_components:
        return connected_components[name]
    else:
        cmp = Rembus(name)
        await cmp.connect()
        add_component(name, cmp)
        cmp.task = asyncio.create_task(component_task(cmp))
        cmp.task.add_done_callback(background_tasks.discard)
        return cmp

class Rembus:
    def __init__(self, name=None):
        self.name = name
        self.ws = None
        self.receiver = None
        self.component = Component(name)
        self.inbox = asyncio.Queue()

        # outstanding requests
        self.outreq = {}
        self.handler = {}

    async def evaluate(self, topic, data):
        """Invoke the handler associate with the message topic.

        :meta private:
        """
        if isinstance(data, list):
            output = await self.handler[topic](*data)
        elif isinstance(data, bytes):
            args = list(data)
            output = await self.handler[topic](*args)
        else:
            output = await self.handler[topic](data)
        return output

    async def parse_input(self, msg):
        """:meta private:"""
        type_byte, msgid = msg[0:2]

        type = type_byte & 0x3F
        flags = type_byte & 0xC0
        logger.debug(f"recv packet type {type}, flags:{flags}")

        if type == TYPE_PUB:
            data = tag2df(msg[2])
            try:
                await self.evaluate(msgid, data)
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
                    output = await self.evaluate(topic, data)
                except Exception as e:
                    status = METHOD_EXCEPTION
                    output = f"{e}"
                    logger.info(f"exception: {e}")

                outmsg = [TYPE_RESPONSE, msgid, status, df2tag(output)]
                logger.debug(msg_str('out', outmsg))

            await self.ws.send(cbor2.dumps(outmsg))
            return

        fut = self.outreq.pop(msgid, None)
        if fut == None:
            logger.warning(f"recv unknown msg id {tohex(msgid)}")
            return

        if type == TYPE_RESPONSE:
            sts = msg[2]
            payload = (msg[3:] + [None])[0]
            if sts == OK:
                fut.set_result(tag2df(payload))
            elif sts == CHALLENGE:
                fut.set_result(payload)
            else:
                fut.set_exception(RembusError(sts, payload))

    async def receive(self):
        """:meta private:"""
        try:
            while True:
                result = await self.ws.recv()
                msg = cbor2.loads(result)
                logger.debug(msg_str('in', msg))
                await self.parse_input(msg)
        except websockets.ConnectionClosedOK:
            logger.debug("connection closed")
        except Exception as e:
            logger.warning(f"closing: {e}")
        finally:
            # for now send a message to task
            await self.inbox.put("reconnect")

    async def connect(self):
        """Connect to the broker."""
        broker_url = self.component.connection_url()

        ssl_context = None
        if self.component.scheme == "wss":
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ca_crt = os.getenv("HTTP_CA_BUNDLE", "rembus-ca.crt")
            if os.path.isfile(ca_crt):
                ssl_context.load_verify_locations(ca_crt)
            else:
                logger.warning(f"CA file not found: {ca_crt}")

        self.ws = await websockets.connect(broker_url, ssl=ssl_context)
        self.receiver = asyncio.create_task(self.receive())

        if self.component.name != None:
            try:
                await self.login()
            except Exception as e:
                raise RembusError("login failed")
        return self

    async def send_wait(self, builder):
        """:meta private:"""
        if self.ws is None:
            raise RembusConnectionClosed()
        reqid = id()
        req = builder(reqid)  
        kid = bytes(reqid)
        await self.ws.send(req)
        self.outreq[kid] = asyncio.get_running_loop().create_future()
        try:
            async with timeout(3):
                return await self.outreq[kid]
        except TimeoutError:
            raise RembusTimeout()

    async def login(self):
        """:meta private:"""
        challenge = await self.send_wait(
            lambda id: encode([TYPE_IDENTITY, id, self.component.name])
        )
        if challenge and isinstance(challenge, bytes):
            logger.debug(f"challenge: {challenge}")
            plain = [bytes(challenge), self.component.name]
            message = cbor2.dumps(plain)
            logger.debug(f"message: {message.hex()}")
            self.privatekey = load_private_key(self.component.name)
            signature = self.privatekey.sign(
                message, padding.PKCS1v15(), hashes.SHA256()
            )
            await self.send_wait(
                lambda id: encode(
                    [TYPE_ATTESTATION, id, self.component.name, signature])
            )
        else:
            logger.debug(f"cid {self.component.name}: free mode access")

    async def publish(self, topic, *args):
        data = df2tag(args)
        await self.ws.send(encode([TYPE_PUB, topic, data]))

    async def broker_setting(self, command, args={}):
        data = {COMMAND: command} | args
        return await self.send_wait(
            lambda id: encode([TYPE_ADMIN, id, BROKER_CONFIG, data])
        )

    async def setting(self, topic, command, args={}):
        data = {COMMAND: command} | args
        return await self.send_wait(lambda id: encode([TYPE_ADMIN, id, topic, data]))

    async def rpc(self, topic, *args):
        data = df2tag(args)
        return await self.send_wait(
            lambda id: encode([TYPE_RPC, id, topic, None, data])
        )

    async def direct(self, target, topic, *args):
        data = df2tag(args)
        return await self.send_wait(
            lambda id: encode([TYPE_RPC, id, topic, target, data])
        )

    async def register(self, cid, pin, tenant=None):
        try:
            privkey = create_private_key()
            pubkey = pem_public_key(privkey)

            response = await self.send_wait(
                lambda id: encode([TYPE_REGISTER, regid(id, pin), cid, tenant, pubkey, 1]))

            if response.status == OK:
                logger.info(f"cid {cid} registered")
                save_private_key(cid, privkey)
        except Exception as e:
            logger.error(f"cid {cid} registration failed: {e}")
            raise e
            
        return None

    async def reactive(self):
        await self.broker_setting("reactive", {"status": True})
        return self

    async def unreactive(self):
        await self.broker_setting("reactive", {"status": False})
        return self

    async def subscribe(self, fn, retroactive=False):
        topic = fn.__name__
        await self.setting(topic, ADD_INTEREST, {"retroactive": retroactive})
        self.handler[topic] = fn
        return self

    async def unsubscribe(self, fn):
        if isinstance(fn, str):
            topic = fn
        else:
            topic = fn.__name__

        await self.setting(topic, REMOVE_INTEREST)
        self.handler.pop(topic, None)
        return self

    async def expose(self, fn):
        topic = fn.__name__
        self.handler[topic] = fn
        await self.setting(topic, ADD_IMPL)

    async def unexpose(self, fn):
        if isinstance(fn, str):
            topic = fn
        else:
            topic = fn.__name__

        self.handler.pop(topic, None)
        await self.setting(topic, REMOVE_IMPL)

    async def shutdown(self):
        await self.inbox.put("shutdown")
        await self.close()
        
    async def close(self):
        remove_component(self.name)

        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error in task: {e}")
            self.task = None

        if self.ws:
            await self.ws.close()
            self.ws = None

        if self.receiver:
            self.receiver.cancel()
            try:
                await self.receiver 
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error in receiver: {e}")
            self.receiver = None


    async def forever(self):
        await self.reactive()
        await self.task

class node:
    def __init__(self, name=None):
        self._runner = AsyncLoopRunner()
        self._rb = self._runner.run(component(name))

    def rpc(self, topic, *args):
        return self._runner.run(self._rb.rpc(topic, *args))

    def publish(self, topic, *args):
        return self._runner.run(self._rb.publish(topic, *args))

    def subscribe(self, fn, retroactive=False):
        return self._runner.run(self._rb.subscribe(fn, retroactive))

    def expose(self, fn):
        return self._runner.run(self._rb.expose(fn))

    def shutdown(self):
        return self._runner.run(self._rb.shutdown())
    
    def close(self):
        try:
            self.shutdown()
        except Exception:
            pass
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

