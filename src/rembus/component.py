import asyncio
import atexit
from async_timeout import timeout
import cbor2
from collections.abc import Coroutine
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
import logging
import os
import pandas as pd
from platformdirs import user_config_dir
import pyarrow as pa
import ssl
import threading
from types import TracebackType
from typing import Callable, Any, Awaitable, List, Optional, Type
import uuid
from urllib.parse import urlparse
import websockets

WS_FRAME_MAXSIZE = 60 * 1024 * 1024

TYPE_IDENTITY = 0
TYPE_PUB = 1
TYPE_RPC = 2
TYPE_ADMIN = 3
TYPE_RESPONSE = 4
TYPE_ACK = 6
TYPE_UNREGISTER = 9
TYPE_REGISTER = 10
TYPE_ATTESTATION = 11

OK = 0
ERROR = 0x0A
CHALLENGE = 0x0B            # 11
IDENTIFICATION_ERROR = 0X14 # 20
METHOD_EXCEPTION = 0X28     # 40
METHOD_ARGS_ERROR = 0X29    # 41
METHOD_NOT_FOUND = 0X2A     # 42
METHOD_UNAVAILABLE = 0X2B   # 43
METHOD_LOOPBACK = 0X2C      # 44
TARGET_NOT_FOUND = 0X2D     # 45
TARGET_DOWN = 0X2E          # 46
UNKNOWN_ADMIN_CMD = 0X2F    # 47

DATAFRAME_TAG = 80

retcode = {
    OK: 'OK',
    ERROR: 'Unknown error',
    IDENTIFICATION_ERROR: 'IDENTIFICATION_ERROR',
    METHOD_EXCEPTION: 'METHOD_EXCEPTION',
    METHOD_ARGS_ERROR: 'METHOD_ARGS_ERROR',
    METHOD_NOT_FOUND: 'METHOD_NOT_FOUND',
    METHOD_UNAVAILABLE: 'METHOD_UNAVAILABLE',
    METHOD_LOOPBACK: 'METHOD_LOOPBACK',
    TARGET_NOT_FOUND: 'TARGET_NOT_FOUND',
    TARGET_DOWN: 'TARGET_DOWN',
    UNKNOWN_ADMIN_CMD: 'UNKNOWN_ADMIN_CMD',
}

BROKER_CONFIG = '__config__'
COMMAND = 'cmd'
ADD_INTEREST = 'subscribe'
REMOVE_INTEREST = 'unsubscribe'
ADD_IMPL = 'expose'
REMOVE_IMPL = 'unexpose'

class RembusException(Exception):
    pass

class RembusTimeout(RembusException):
    def __str__(self):
        return 'request timeout'

class RembusConnectionClosed(RembusException):
    def __str__(self):
        return 'connection down'

class RembusError(RembusException):
    def __init__(self, status_code:int, msg:str|None=None):
        self.status = status_code
        self.message = msg

    def __str__(self):
        if self.message:
            return f'{retcode[self.status]}:{self.message}'
        else:
            return f'{retcode[self.status]}'

def randname() -> str:
    """Return a random name for a component."""
    return str(uuid.uuid4())

def request_timeout():
    return float(os.environ.get('REMBUS_TIMEOUT', 10.0))


def msg_status(response:list[Any]):
    """Return the status code of the response message."""
    return response[2]

def msg_id(response:list[Any]):
    """Return the message id of the response message."""
    return response[1]

logger = logging.getLogger("rembus")

_loop_runner = None

logging.basicConfig(level=logging.ERROR)

logger = logging.getLogger("rembus")

def tohex(bytes:bytes):
    """Return a string with bytes as hex numbers with 0xNN format."""
    return ' '.join(f'0x{x:02x}' for x in bytes)


def field_repr(bstr:bytes|str):
    """String repr of the second field of a rembus message.

    The second field may be a 16-bytes message unique id or a topic string
    value.  
    """
    return tohex(bstr) if isinstance(bstr, bytes) else bstr


def msg_str(dir:str, msg:list[Any]):
    """Return a printable dump of rembus message `msg`."""
    payload = ", ".join(str(el) for el in msg[2:])
    s = f'{dir}: [{msg[0]}, {field_repr(msg[1])}, {payload}]'
    return s

class Component:

    def __init__(self, url:str|None=None) -> None:
        uri = urlparse(url)
        if isinstance(uri.path, str):
            self.hasname = True
            self.name = uri.path[1:] if uri.path.startswith("/") else uri.path
        else:
            self.hasname = False
            self.name = randname()
        if uri.scheme:
            self.scheme = uri.scheme
        else:
            self.scheme = "ws"
        if uri.netloc:
            self.netloc = uri.netloc
        else:
            self.netloc = os.getenv('REMBUS_BASE_URL', 'localhost:8000')

    def connection_url(self):
        if self.name:
            return f"{self.scheme}://{self.netloc}/{self.name}"
        else:
            return f"{self.scheme}://{self.netloc}"


def decode_dataframe(data:bytes) -> pd.DataFrame:
    """Decode a CBOR tagged value `data` to a pandas dataframe."""
    writer = pa.BufferOutputStream()
    writer.write(data)
    buf: pa.Buffer = writer.getvalue() 
    reader = pa.ipc.open_stream(buf)
    with pa.ipc.open_stream(buf) as reader:
        return reader.read_pandas()


def encode_dataframe(df:pd.DataFrame) -> cbor2.CBORTag:
    """Encode a pandas dataframe `df` to a CBOR tag value."""
    table = pa.Table.from_pandas(df)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write(table)
    buf = sink.getvalue()
    stream = pa.input_stream(buf)
    return cbor2.CBORTag(DATAFRAME_TAG, stream.read())


def encode(msg:list[Any]) -> bytes:
    """Encode message `msg`."""
    logger.debug(msg_str('out', msg))
    return cbor2.dumps(msg)


def tag2df(data: Any) -> Any:
    """Loop over `data` items and decode tagged values to dataframes."""
    if isinstance(data, list):
        for idx, val in enumerate(data):
            if isinstance(val, cbor2.CBORTag) and val.tag == DATAFRAME_TAG:
                data[idx] = decode_dataframe(val.value)
    elif isinstance(data, cbor2.CBORTag):
        return decode_dataframe(data.value)
    return data


def df2tag(data:Any)-> Any:
    """Loop over `data` items and encode dataframes to tag values."""
    if isinstance(data, tuple):
        lst: List[Any] = []
        for idx, val in enumerate(data):
            if isinstance(val, pd.DataFrame):
                lst.append(encode_dataframe(val))
            else:
                lst.append(val)
        return lst
    elif isinstance(data, list):
        for idx, val in enumerate(data):
            if isinstance(val, pd.DataFrame):
                data[idx] = encode_dataframe(val)

    elif isinstance(data, pd.DataFrame):
        data = encode_dataframe(data)
    return data

def regid(id: bytearray, pin: str) -> bytearray:
    bpin = bytes.fromhex(pin[::-1])
    id[:4] = bpin[:4]
    return id

def id():
    """Return an array of 16 random bytes."""
    return bytearray(os.urandom(16))


def config_dir():
    """The directory for rembus secrets."""
    # appears in path only for Windows machine
    app_author = "Rembus"
    return user_config_dir("rembus", app_author)

def create_private_key():
    return rsa.generate_private_key(public_exponent=65537,key_size=2048
)

def pem_public_key(private_key:PrivateKeyTypes) -> bytes:
    return private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

def save_private_key(cid:str, private_key:PrivateKeyTypes):
    dir = os.path.join(config_dir(), cid)
    
    if not os.path.exists(dir):
        os.makedirs(dir)

    fn = os.path.join(dir, ".secret")
    private_key_file = open(fn, "wb")
 
    pem_private_key = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()
    )

    private_key_file.write(pem_private_key)
    private_key_file.close()


def load_private_key(cid:str) -> PrivateKeyTypes:
    fn = os.path.join(config_dir(), cid, ".secret")
    #fn = os.path.join(config_dir(), cid)
    with open(fn, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(), password=None)

    return private_key


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

    def run(self, coro: Coroutine[Any, Any, Any]) -> Any:
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result()
    
    def shutdown(self):
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            self._thread.join()

async def get_response(obj:Any) -> Any:
    """Return the response of the object."""
    if asyncio.iscoroutine(obj):
        return await obj
    else:
        return obj

class Rembus:
    def __init__(self, name:str|None=None):
        self.ws: websockets.ClientConnection | None = None
        self.receiver = None
        self.component = Component(name)
        self.inbox: asyncio.Queue[str] = asyncio.Queue()

        # outstanding requests
        self.outreq: dict[bytes, asyncio.Future[Any]] = {}
        self.handler: dict[str, Callable[..., Awaitable[Any]]] = {}
        self.task: Optional[asyncio.Task[None]] = None

    def isopen(self) -> bool:
        """Check if the connection is open."""
        return self.ws is not None and self.ws.state == websockets.State.OPEN 

    async def evaluate(self, topic:str, data:Any) -> Any:
        """Invoke the handler associate with the message topic.

        :meta private:
        """
        if isinstance(data, list):
            output = await get_response(self.handler[topic](*data))
        elif isinstance(data, bytes):
            args = list(data)
            output = await get_response(self.handler[topic](*args))
        else:
            output = await get_response(self.handler[topic](data))
        return output

    async def parse_input(self, msg: list[Any]):
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

                outmsg: Any = [TYPE_RESPONSE, msgid, status, df2tag(output)]
                logger.debug(msg_str('out', outmsg))
            
            if self.ws is None:
                logger.error("connection down")
                return
            
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
            while True and self.ws is not None:
                result:str|bytes = await self.ws.recv()
                if isinstance(result, str):
                    raise RembusError(ERROR, "unexpected text message")
                msg:list[Any] = cbor2.loads(result)
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

        if self.component.hasname:
            try:
                await self.login()
            except Exception as e:
                raise RembusError(ERROR, "login failed")
        return self

    async def send_wait(self, builder: Callable[[bytearray], bytes]) -> Any:
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
            privatekey = load_private_key(self.component.name)
            signature:bytes = privatekey.sign(
                message, padding.PKCS1v15(), hashes.SHA256()
            )
            await self.send_wait(
                lambda id: encode(
                    [TYPE_ATTESTATION, id, self.component.name, signature])
            )
        else:
            logger.debug(f"cid {self.component.name}: free mode access")

    async def publish(self, topic:str, *args:tuple[Any]):
        data = df2tag(args)
        if self.ws is None:
            raise RuntimeError("connection down")
        await self.ws.send(encode([TYPE_PUB, topic, data]))

    async def broker_setting(self, command:str, args:dict[str,Any]={}):
        data = {COMMAND: command} | args
        return await self.send_wait(
            lambda id: encode([TYPE_ADMIN, id, BROKER_CONFIG, data])
        )

    async def setting(self, topic:str, command:str, args:dict[str,Any]={}):
        data = {COMMAND: command} | args
        return await self.send_wait(lambda id: encode([TYPE_ADMIN, id, topic, data]))

    async def rpc(self, topic:str, *args:tuple[Any]):
        data = df2tag(args)
        return await self.send_wait(
            lambda id: encode([TYPE_RPC, id, topic, None, data])
        )

    async def direct(self, target:str, topic:str, *args:tuple[Any]):
        data = df2tag(args)
        return await self.send_wait(
            lambda id: encode([TYPE_RPC, id, topic, target, data])
        )

    async def register(self, cid:str, pin:str, tenant:str|None=None):
        try:
            privkey = create_private_key()
            pubkey = pem_public_key(privkey)
            response = await self.send_wait(
                lambda id: encode(
                    [TYPE_REGISTER, regid(id, pin), cid, tenant, pubkey, 1]
                ))

            logger.info(f"cid {cid} registered")
            save_private_key(cid, privkey)
        except Exception as e:
            logger.error(f"cid {cid} registration failed: {e}")
            raise e
            
        return response
    
    async def unregister(self):
        return await self.send_wait(
            lambda id: encode([TYPE_UNREGISTER, id])
        )

    async def reactive(self):
        await self.broker_setting("reactive", {"status": True})
        return self

    async def unreactive(self):
        await self.broker_setting("reactive", {"status": False})
        return self

    async def subscribe(self, fn:Callable[..., Any], retroactive:bool=False):
        topic = fn.__name__
        await self.setting(topic, ADD_INTEREST, {"retroactive": retroactive})
        self.handler[topic] = fn
        return self

    async def unsubscribe(self, fn:Callable[..., Any]):
        if isinstance(fn, str):
            topic = fn
        else:
            topic = fn.__name__

        await self.setting(topic, REMOVE_INTEREST)
        self.handler.pop(topic, None)
        return self

    async def expose(self, fn:Callable[..., Any]):
        topic = fn.__name__
        self.handler[topic] = fn
        await self.setting(topic, ADD_IMPL)

    async def unexpose(self, fn:Callable[..., Any]):
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
        remove_component(self.component.name)

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
        if self.task is not None:
            await self.task

class node:
    def __init__(self, name:str|None=None):
        self._runner = AsyncLoopRunner()
        self._rb = self._runner.run(component(name))

    def isopen(self) -> bool:
        """Check if the connection is open."""
        return self._rb.isopen()
    
    def register(self, cid:str, pin:str, tenant:str|None=None):
        return self._runner.run(self._rb.register(cid, pin, tenant))

    def unregister(self):
        return self._runner.run(self._rb.unregister())

    def rpc(self, topic:str, *args:tuple[Any]):
        return self._runner.run(self._rb.rpc(topic, *args))

    def publish(self, topic:str, *args:tuple[Any]):
        return self._runner.run(self._rb.publish(topic, *args))

    def subscribe(self, fn:Callable[..., Any], retroactive:bool=False):
        return self._runner.run(self._rb.subscribe(fn, retroactive))

    def expose(self, fn:Callable[..., Any]):
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

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]) -> Optional[bool]:
        self.close()

# Global dictionary to store connected components
_components: dict[str, Rembus]  = {}

def add_component(name:str, component:Rembus):
    """Add a component to the connected components dictionary."""
    _components[name] = component

def get_component(name:str)->Rembus|None:
    """Retrieve a component from the connected components dictionary."""
    return _components.get(name)

def remove_component(name:str):
    """Remove a component from the connected components dictionary."""
    if name in _components:
        del _components[name]

async def component_task(cmp:Rembus):
    while True:
        msg: str = await cmp.inbox.get()
        logger.debug(f"component task: {msg}")
        if msg == "shutdown":
            break

        logger.debug(f"{cmp.component.name}: reconnecting ...")
        try:
            await cmp.connect()
            await cmp.reactive()
        except Exception as e:
            logger.info(f"{cmp.component.name} connect: {e}")
            await asyncio.sleep(2)
            cmp.inbox.put_nowait("reconnect")

async def component(name:str|None):
    if name in _components:
        return _components[name]
    else:
        cmp = Rembus(name)
        await cmp.connect()
        add_component(cmp.component.name, cmp)
        cmp.task = asyncio.create_task(component_task(cmp))
        return cmp

def register(cid:str, pin:str, tenant:str|None=None):
    rb = node()
    rb.register(cid, pin, tenant)
    rb.close()
