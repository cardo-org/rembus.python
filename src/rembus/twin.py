import asyncio
from websockets.asyncio.server import serve
from async_timeout import timeout
import atexit
import cbor2
from collections.abc import Coroutine
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
import logging
import os
import pandas as pd
import pyarrow as pa
import ssl
import threading
import time
from types import TracebackType
from typing import Callable, Any, Awaitable, List, Optional, Type
import uuid
from urllib.parse import urlparse
import websockets

from .settings import Config, rembus_dir
from .store import (
    key_file,
    keys_dir,
    load_tenants,
    load_tenant_component,
    isregistered,
    save_tenant_component,
    save_pubkey
)

from .protocol import (
    SIG_RSA,
    SIG_ECDSA,
    QOS0,
    QOS1,
    QOS2,
    WS_FRAME_MAXSIZE,
    TYPE_IDENTITY,
    TYPE_PUB,
    TYPE_RPC,
    TYPE_ADMIN,
    TYPE_RESPONSE,
    TYPE_ACK,
    TYPE_ACK2,
    TYPE_UNREGISTER,
    TYPE_REGISTER,
    TYPE_ATTESTATION,
    STS_OK,
    STS_ERROR,
    STS_CHALLENGE,
    STS_IDENTIFICATION_ERROR,
    STS_METHOD_EXCEPTION,
    STS_METHOD_ARGS_ERROR,
    STS_METHOD_NOT_FOUND,
    STS_METHOD_UNAVAILABLE,
    STS_METHOD_LOOPBACK,
    STS_TARGET_NOT_FOUND,
    STS_TARGET_DOWN,
    STS_UNKNOWN_ADMIN_CMD,
    STS_NAME_ALREADY_TAKEN,
    DATAFRAME_TAG,
    BROKER_CONFIG,
    COMMAND,
    ADD_INTEREST,
    REMOVE_INTEREST,
    ADD_IMPL,
    REMOVE_IMPL,
    id,
    bytes2id,
    RembusException,
    RembusTimeout,
    RembusConnectionClosed,
    RembusError,
    AdminMsg,
    IdentityMsg,
    RegisterMsg
)

__all__ = ["component", "RbURL"]

def randname() -> str:
    """Return a random name for a component."""
    return str(uuid.uuid4())

#logging.basicConfig(level=logging.ERROR)
logging.basicConfig(level=logging.INFO)

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

def create_private_key():
    return rsa.generate_private_key(public_exponent=65537,key_size=2048
)

def pem_public_key(private_key:PrivateKeyTypes) -> bytes:
    return private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

def save_private_key(cid:str, private_key:PrivateKeyTypes):
    dir = os.path.join(rembus_dir(), cid)
    
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
    fn = os.path.join(rembus_dir(), cid, ".secret")
    #fn = os.path.join(rembus_dir(), cid)
    with open(fn, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(), password=None)

    return private_key

async def get_response(obj:Any) -> Any:
    """Return the response of the object."""
    if asyncio.iscoroutine(obj):
        return await obj
    else:
        return obj

class FutureResponse:
    def __init__(self, data:Any=None):
        self.future: asyncio.Future = asyncio.get_running_loop().create_future()
        self.data = data

def getargs(data):
    if isinstance(data, list):
        return data
    else:
        return [data]   

class RbURL:
    def __init__(self, url:str|None=None) -> None:
        baseurl = urlparse(os.getenv('REMBUS_BASE_URL', "ws://127.0.0.1:8000"))
        uri = urlparse(url)

        if uri.scheme == "repl":
            self.protocol = uri.scheme
            self.hostname = ''
            self.port = 0
            self.hasname = False
        else:
            if isinstance(uri.path, str) and uri.path:
                self.hasname = True
                self.id = uri.path[1:] if uri.path.startswith("/") else uri.path
            else:
                self.hasname = False
                self.id = randname()
            
            if uri.scheme:
                self.protocol = uri.scheme
            else:
                self.protocol = baseurl.scheme
            
            if uri.hostname:
                self.hostname = uri.hostname
            else:
                self.hostname = baseurl.hostname
            
            if uri.port:
                self.port = uri.port
            else:
                self.port = baseurl.port

    def rid(self):
        """Return the unique id of the component."""
        return self.id

    def connection_url(self):
        if self.hasname:
            return f"{self.protocol}://{self.hostname}:{self.port}/{self.id}"
        else:
            return f"{self.protocol}://{self.hostname}:{self.port}"

class Router:
    def __init__(self, name:str):
        self.id = name
        self.id_twin: dict[str, Twin] = {}
        self.handler: dict[str, Callable[..., Any]] = {}
        self.inbox: asyncio.Queue[Any] = asyncio.Queue()
        self.shared: Any = None
        self.handler["rid"] = lambda: self.id
        self.task: asyncio.Task[None] = asyncio.create_task(self.router_task())
        self.serve_task: Optional[asyncio.Task[None]] = None
        self.config = Config(name)
        self.owners = load_tenants(self)
        self.component_owners = load_tenant_component(self)

    def __str__(self):
        return f"{self.id}"

    def __repr__(self):
        return f"{self.id}: {self.id_twin}"

    def bind(self, ws, url:RbURL, isclient:bool=True):
        """Bind the socket to a Twin."""
        rid = url.rid()
        if rid in self.id_twin:
            twin = self.id_twin[rid]
        else:
            twin = Twin(url, self, isclient)
            self.id_twin[rid] = twin
            
        twin.socket = ws    
        return twin

    def isconnected(self, cid:str) -> bool:
        return cid in self.id_twin

    async def router_task(self):
        logger.debug(f"[{self.id}] router started")
        while True:
            msg = await self.inbox.get()
            if msg == "shutdown":
                logger.warning(f"[{self.id}] router shutdown")
                if self.serve_task:
                    self.serve_task.cancel()
                    try:
                        await self.serve_task
                    except asyncio.CancelledError:
                        pass
                break
            elif isinstance(msg, IdentityMsg):
                twin_id = msg.cid
                sts = STS_OK
                if self.isconnected(twin_id):
                    sts = STS_ERROR
                    logger.warning(f"[{self}] node with id [$twin_id] is already connected")
                    await msg.twin.close()
                else:
                    logger.debug(f"[{self}] identity: {msg.cid}")
                    await self.auth_identity(msg)
                
                await msg.twin.response(sts, msg)
            elif isinstance(msg, AdminMsg):
                logger.debug(f"[{self.id}] admin: {msg}")
                await msg.twin.response(STS_OK, msg)
            elif isinstance(msg, RegisterMsg):
                logger.debug(f"[{self.id}] register: {msg}")
                await self.register_node(msg)
            else:
                # Process other messages
                pass


    async def evaluate(self, twin, topic:str, data:Any) -> Any:
        """Invoke the handler associate with the message topic.

        :meta private:
        """
        if self.shared:
            output = await get_response(
                self.handler[topic](self.shared, twin, *getargs(data))
            )
        else:
            output = await get_response(self.handler[topic](*getargs(data)))

        return output
    
    async def client_receiver(self, ws):
        """Receive messages from the client component."""
        twin = self.bind(ws, RbURL(), False)
        await twin.twin_receiver()

    async def serve_ws(self, port:int, issecure:bool=False):
        async with serve(self.client_receiver, "0.0.0.0", port) as server:
            await server.serve_forever()

    def if_authenticated(self, cid:str):
        """Check if the component needs authentication."""
        return key_file(self.id, cid) is not None

    async def auth_identity(self, msg:IdentityMsg):
        """Authenticate the identity of the component."""
        twin = msg.twin
        identity = msg.cid

        if self.if_authenticated(identity):
            # cid is registered, send the challenge
            pass
        else:
            if twin.rid != identity:
                logger.info(f"identity mismatch: {twin.rid} != {identity}")
                self.id_twin[identity] = self.id_twin.pop(twin.rid, twin)
                twin.rid = identity
                ### raise RembusError(STS_IDENTIFICATION_ERROR, "identity mismatch")
            else:
                logger.debug(f"[{self.id}] identity: {identity}")

    def isenabled(self, msg:RegisterMsg):
        """Check if the component is enabled."""
        return True

    def get_token(self, tenant, id:bytes):
        #vals = UInt8[(id>>24)&0xff, (id>>16)&0xff, (id>>8)&0xff, id&0xff]
        vals = id[3::-1]
        
        #token = bytes2hex(vals)
        token = vals.hex()

        df = self.owners[(self.owners.pin==token) & (self.owners.tenant==tenant)]
        if df.empty:
            logger.info(f"tenant {tenant}: invalid token {token}")
            return None
        else:
            logger.debug(f"tenant {tenant}: token is valid")
            return token

    async def register_node(self, msg: RegisterMsg):
        """Set the node secret."""
        sts = STS_OK
        reason = None
        if not self.isenabled(msg):
            sts = STS_ERROR
        else:
            token = self.get_token(msg.tenant, msg.id)
            if token is None:
                sts = STS_ERROR
                reason = "wrong tenant/pin"
            elif isregistered(self.id, msg.cid):
                sts = STS_NAME_ALREADY_TAKEN
                reason = f"name {msg.cid} not available for registration"
            else:
                kdir = keys_dir(self.id)
                os.makedirs(kdir, exist_ok=True)
                save_pubkey(self.id, msg.cid, msg.pubkey, msg.type)
                
                self.component_owners.loc[len(self.component_owners)] = [msg.tenant, msg.cid]
                
                save_tenant_component(self.id, self.component_owners)
                logger.debug(f"cid {msg.cid} registered")

        await msg.twin.response(sts, msg, reason)

class Twin:
    def __init__(self, uid:RbURL, router:Router, isclient:bool=True):
        self.isclient = isclient
        self._router = router
        self.socket: websockets.ClientConnection | None = None
        self.receiver = None
        self.uid = uid
        self.inbox: asyncio.Queue[str] = asyncio.Queue()

        # outstanding requests
        self.outreq: dict[bytes, FutureResponse] = {}
        self.task: Optional[asyncio.Task[None]] = None
        self.ackdf: dict[int, int] = {} # msgid => ts

    def __str__(self):
        return self.uid.id

    def __repr__(self):
        return self.uid.id

    @property
    def rid(self):
        return self.uid.id

    @rid.setter
    def rid(self, rid:str):
        self.uid.id = rid

    @property
    def router(self):
        return self._router

    @router.setter
    def router(self, router):
        self._router = router

    def isrepl(self) -> bool:
        """Check if twin is a REPL"""
        return self.uid.protocol == "repl"

    def isopen(self) -> bool:
        """Check if the connection is open."""
        return self.socket is not None and self.socket.state == websockets.State.OPEN 

    async def response(self, status:int, msg:Any, data:Any=None):
        """Send a response to the client."""
        if self.socket is None:
            raise RembusConnectionClosed()
        
        outmsg: Any = [TYPE_RESPONSE, msg.id, status, data]
        await self.send(cbor2.dumps(outmsg))

    def inject(self, data:Any):
        """Initialize the context object."""
        self.router.shared = data

    async def twin_task(self):
        logger.info(f"component task {self.uid.id} started {self}")
        reconnect_task = None
        while True:
            msg: str = await self.inbox.get()
            logger.debug(f"component_task: {msg}")
            if msg == "shutdown":
                if reconnect_task:
                    reconnect_task.cancel()
                if self.isclient:
                    await self.router.inbox.put("shutdown")
                break
            elif msg == "reconnect":
                logger.debug(f"{self.uid.id}: reconnecting ...")
                try:
                    await self.connect()
                    await self.reactive()
                    reconnect_task = None
                except Exception as e:
                    logger.info(f"{self.uid.id} component_task error: {e}")
                    await asyncio.sleep(2)
                    reconnect_task = delayed_put(self.inbox, "reconnect", 2)

    async def twin_receiver(self):
        logger.debug(f"{self.uid.id} client is connected")
        try:
            while True and self.socket is not None:
                result:str|bytes = await self.socket.recv()
                if isinstance(result, str):
                    raise RembusError(STS_ERROR, "unexpected text message")
                msg:list[Any] = cbor2.loads(result)
                logger.debug(msg_str('in', msg))
                await self.parse_input(msg)
        except websockets.ConnectionClosedOK:
            logger.debug("connection closed")
        except Exception as e:
            logger.warning(f"connection closed ({type(e)}): {e}")
        finally:
            if self.isclient:
                logger.debug(f"{self.rid} twin_receiver done: {self.task}")
                await self.inbox.put("reconnect")
            else:
                logger.debug(f"{self.rid} twin_receiver pop: {self.rid}")
                self.router.id_twin.pop(self.rid, None)

    async def future_request(self, msgid:bytes):
        """Return the future associated with the message id `msgid`."""
        fut = self.outreq.pop(msgid, None)
        if fut == None:
            logger.warning(f"recv unknown msg id {tohex(msgid)}")
        return fut

    async def parse_input(self, msg: list[Any]):
        """:meta private:"""
        type_byte, msgid = msg[0:2]

        type = type_byte & 0x0F
        flags = type_byte & 0xF0
        #logger.debug(f"recv {msg} type {type}, flags:{flags}")

        if type == TYPE_PUB:
            if flags > QOS0:
                topic = msg[2]
                data = msg[3]
                #logger.debug(f'[{self.uid.id}] pubsub ack [{bytes2id(msgid)}]')
                if self.socket:
                    await self.socket.send(cbor2.dumps([TYPE_ACK, msgid]))
                if flags == QOS2:
                    id = bytes2id(msgid)
                    if id in self.ackdf:
                        # Already received, skip the message.
                        return
                    else:
                        # Save the message id to guarantee exactly one delivery.
                        self.ackdf[id] = int(time.time())
            else:
                topic = msgid
                data = tag2df(msg[2])

            try:
                await self.router.evaluate(self, topic, data)
            except Exception as e:
                logger.error(f"subscribe error: {e}")
            
            return
        elif type == TYPE_RPC:
            data = tag2df(msg[4])
            topic = msg[2]

            if not topic in self.router.handler:
                outmsg = [TYPE_RESPONSE, msgid, STS_METHOD_NOT_FOUND, topic]
            else:
                status = STS_OK
                try:
                    output = await self.router.evaluate(self, topic, data)
                except Exception as e:
                    status = STS_METHOD_EXCEPTION
                    output = f"{e}"
                    logger.info(f"exception: {e}")

                outmsg: Any = [TYPE_RESPONSE, msgid, status, df2tag(output)]
                logger.debug(msg_str('out', outmsg))
            
            await self.send(cbor2.dumps(outmsg))
            return
        elif type == TYPE_ACK2:
            id = bytes2id(msg[1])
            if id in self.ackdf:
                logger.debug(f"deleting pubsub ack: {id}")
                del self.ackdf[id]
            return
        elif type == TYPE_UNREGISTER:
            pass
        elif type == TYPE_REGISTER:
            self.router.inbox.put_nowait(RegisterMsg(self, msg))
        elif type == TYPE_IDENTITY:
            logger.debug(f"[{self}] identity: {msg[2]}")
            self.router.inbox.put_nowait(IdentityMsg(self, msg))
        elif type == TYPE_ADMIN:
            self.router.inbox.put_nowait(AdminMsg(self, msg))
        elif type == TYPE_RESPONSE:
            fut = await self.future_request(msgid)
            if fut:
                sts = msg[2]
                payload = (msg[3:] + [None])[0]
                if sts == STS_OK:
                    fut.future.set_result(tag2df(payload))
                elif sts == STS_CHALLENGE:
                    fut.future.set_result(payload)
                else:
                    fut.future.set_exception(RembusError(sts, payload))
        elif type == TYPE_ACK:
            logger.debug(f"pubsub ack data")
            fut = await self.future_request(msgid)
            if fut:
                if fut.data:
                    # fut.data is true if the message is a QOS2
                    await self.send(encode([TYPE_ACK2, msg[1]]))
                fut.future.set_result(True)

##     async def receive(self):
##         """:meta private:"""
##         try:
##             while True and self.socket is not None:
##                 result:str|bytes = await self.socket.recv()
##                 if isinstance(result, str):
##                     raise RembusError(STS_ERROR, "unexpected text message")
##                 msg:list[Any] = cbor2.loads(result)
##                 logger.debug(msg_str('in', msg))
##                 await self.parse_input(msg)
##         except websockets.ConnectionClosedOK:
##             logger.debug("connection closed")
##         except Exception as e:
##             logger.warning(f"connection closed: {e}")
##         finally:
##             # for now send a message to task
##             await self.inbox.put("reconnect")

    async def connect(self):
        """Connect to the broker."""
        broker_url = self.uid.connection_url()

        ssl_context = None
        if self.uid.protocol == "wss":
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ca_crt = os.getenv("HTTP_CA_BUNDLE", "rembus-ca.crt")
            if os.path.isfile(ca_crt):
                ssl_context.load_verify_locations(ca_crt)
            else:
                logger.warning(f"CA file not found: {ca_crt}")

        self.socket = await websockets.connect(
            broker_url,
            ping_interval=self.router.config.ws_ping_interval,
            max_size=WS_FRAME_MAXSIZE,
            ssl=ssl_context
        )
        self.receiver = asyncio.create_task(self.twin_receiver())

        if self.uid.hasname:
            try:
                await self.login()
            except Exception as e:
                raise RembusError(STS_ERROR, "login failed")
        return self

    async def send(self, payload: bytes) -> Any:
        if self.socket is None:
            raise RembusConnectionClosed()
        
        await self.socket.send(payload)

    async def send_wait(self, builder: Callable[[bytearray], bytes], data: Any=None) -> Any:
        """:meta private:"""
        reqid = id()
        req = builder(reqid) 
        kid = bytes(reqid)
        await self.send(req)
        futreq = FutureResponse(data)
        self.outreq[kid] = futreq
        try:
            async with timeout(self.router.config.request_timeout):
                return await futreq.future
        except TimeoutError:
            raise RembusTimeout()

    async def login(self):
        """:meta private:"""
        challenge = await self.send_wait(
            lambda id: encode([TYPE_IDENTITY, id, self.uid.id])
        )
        if challenge and isinstance(challenge, bytes):
            logger.debug(f"challenge: {challenge}")
            plain = [bytes(challenge), self.uid.id]
            message = cbor2.dumps(plain)
            logger.debug(f"message: {message.hex()}")
            privatekey = load_private_key(self.uid.id)
            signature:bytes = privatekey.sign(
                message, padding.PKCS1v15(), hashes.SHA256()
            )
            await self.send_wait(
                lambda id: encode(
                    [TYPE_ATTESTATION, id, self.uid.id, signature])
            )
        else:
            logger.debug(f"cid {self.uid.id}: free mode access")

    async def publish(self, topic:str, *args:tuple[Any], **kwargs):
        data = df2tag(args)
        if self.socket is None:
            raise RuntimeError("connection down")
        qos = kwargs.get("qos", QOS0)
        
        if qos == QOS0:
            await self.socket.send(encode([TYPE_PUB|qos, topic, data]))
        else:
            done = False
            while not done:
                try:
                    done = await self.send_wait(
                        lambda id: encode([TYPE_PUB|qos, id, topic, data]),
                        qos == QOS2
                    )
                except RembusTimeout:
                    pass

        return None

    async def broker_setting(self, command:str, args:dict[str,Any]={}):
        data = {COMMAND: command} | args
        return await self.send_wait(
            lambda id: encode([TYPE_ADMIN, id, BROKER_CONFIG, data])
        )

    async def setting(self, topic:str, command:str, args:dict[str,Any]={}):
        if self.socket:
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
                    [TYPE_REGISTER, regid(id, pin), cid, tenant, pubkey, SIG_RSA]
                ))

            logger.debug(f"cid {cid} registered")
            save_private_key(cid, privkey)
        except Exception as e:
            logger.error(f"cid {cid} registration failed: {e}")
            raise e
            
        return response
    
    async def unregister(self):
        await self.send_wait(
            lambda id: encode([TYPE_UNREGISTER, id, self.uid.id])
        )
        os.remove(os.path.join(rembus_dir(), self.uid.id, ".secret"))
        return self

    async def reactive(self):
        await self.broker_setting("reactive", {"status": True})
        return self

    async def unreactive(self):
        await self.broker_setting("reactive", {"status": False})
        return self

    async def subscribe(self, fn:Callable[..., Any], retroactive:bool=False):
        topic = fn.__name__
        await self.setting(topic, ADD_INTEREST, {"retroactive": retroactive})
        self.router.handler[topic] = fn
        return self

    async def unsubscribe(self, fn:Callable[..., Any]):
        if isinstance(fn, str):
            topic = fn
        else:
            topic = fn.__name__

        await self.setting(topic, REMOVE_INTEREST)
        self.router.handler.pop(topic, None)
        return self

    async def expose(self, fn:Callable[..., Any]):
        topic = fn.__name__
        self.router.handler[topic] = fn
        await self.setting(topic, ADD_IMPL)

    async def unexpose(self, fn:Callable[..., Any]):
        if isinstance(fn, str):
            topic = fn
        else:
            topic = fn.__name__

        self.router.handler.pop(topic, None)
        await self.setting(topic, REMOVE_IMPL)

    async def close(self):
        await self.inbox.put("shutdown")
        remove_component(self.uid.id)

        if self.socket:
            await self.socket.close()
            self.socket = None

        if self.receiver:
            self.receiver.cancel()
            try:
                await self.receiver 
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error in receiver: {e}")
            self.receiver = None

    async def wait(self):
        await self.reactive()
        if self.task is not None:
            await self.task

# Global dictionary to store connected components
_components: dict[str, Twin]  = {}

def add_component(name:str, component:Twin):
    """Add a component to the connected components dictionary."""
    _components[name] = component

def get_component(name:str)->Twin|None:
    """Retrieve a component from the connected components dictionary."""
    return _components.get(name)

def remove_component(name:str):
    """Remove a component from the connected components dictionary."""
    if name in _components:
        del _components[name]

async def sleep_put(queue: asyncio.Queue, item, delay: float):
    await asyncio.sleep(delay)
    await queue.put(item)

def delayed_put(queue: asyncio.Queue, item, delay: float):
    return asyncio.create_task(sleep_put(queue, item, delay))

async def component(
        url:str|None = None, name:str='broker', port:int|None= None
    )-> Twin:
    """Return a Rembus component."""
    if url:
        uid = RbURL(url)
    else:
        uid = RbURL("repl://")
        uid.id = name
    
    if uid.id in _components:
        return _components[uid.id]
    else:
        router = Router(name)
        logger.debug(f"component {uid.id} created, port: {port}")
        # start a websocket server
        if port:
            router.serve_task = asyncio.create_task(router.serve_ws(port))
        else:
            router.serve_task = None
        
        cmp = Twin(uid, router)
        if uid.protocol != "repl":
            await cmp.connect()
        
        add_component(cmp.uid.id, cmp)
        cmp.task = asyncio.create_task(cmp.twin_task())
        return cmp
