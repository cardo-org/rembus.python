import asyncio
from async_timeout import timeout
from rembus.common import *

logger = logging.getLogger("rembus")


async def component(name=None):
    return await Rembus(name).connect()


class Rembus:
    def __init__(self, name=None):
        self.ws = None
        self.receiver = None
        self.component = Component(name)

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
            await self.close()

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
                logger.warn(f"CA file not found: {ca_crt}")

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
        reqid = id()
        await self.ws.send(builder(reqid))
        self.outreq[reqid] = asyncio.get_running_loop().create_future()
        try:
            async with timeout(3):
                return await self.outreq[reqid]
        except TimeoutError:
            raise RembusTimeout()

    async def login(self):
        """:meta private:"""
        challenge = await self.send_wait(
            lambda id: encode([TYPE_IDENTITY, id, self.component.name])
        )
        if challenge:
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

    async def close(self):
        self.receiver.cancel()
        if self.ws:
            await self.ws.close()
            self.ws = None

    async def forever(self):
        await self.reactive()
        if self.receiver != None:
            await self.receiver
