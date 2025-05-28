"""
The core module of the Rembus library that includes implementations for the
Router and Twin concepts.
"""
import asyncio
import logging
import os
import time
from typing import Callable, Any, Optional
import ssl
from urllib.parse import urlparse
import uuid
import async_timeout
from websockets.asyncio.server import serve
import cbor2
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa, ec
import websockets
import rembus.protocol as rp
import rembus.settings as rs
from . import __version__

logger = logging.getLogger(__name__)


def domain(s: str) -> str:
    """Return the domain part from the string.

    If no domain is found, return the root domain ".".
    """
    dot_index = s.find('.')
    if dot_index != -1:
        return s[dot_index + 1:]
    else:
        return "."


def randname() -> str:
    """Return a random name for a component."""
    return str(uuid.uuid4())


async def get_response(obj: Any) -> Any:
    """Return the response of the object."""
    if asyncio.iscoroutine(obj):
        return await obj
    else:
        return obj


class FutureResponse:
    """
    Encapsulate a future response for a request.
    """

    def __init__(self, data: Any = None):
        self.future: asyncio.Future = asyncio.get_running_loop().create_future()
        self.data = data


def getargs(data):
    """
    Return arguments list from the data.
    """
    if isinstance(data, list):
        return data
    else:
        return [data]


class RbURL:
    """
    A class to parse and manage Rembus URLs.
    It supports the 'repl' scheme and the standard 'ws'/'wss' schemes.
    """

    def __init__(self, url: str | None = None) -> None:
        baseurl = urlparse(os.getenv('REMBUS_BASE_URL', "ws://127.0.0.1:8000"))
        uri = urlparse(url)

        if uri.scheme == "repl":
            self.protocol = uri.scheme
            self.hostname = ''
            self.port = 0
            self.hasname = False
            self.id = 'repl'
        else:
            if isinstance(uri.path, str) and uri.path:
                self.hasname = True
                self.id = uri.path[1:] if uri.path.startswith(
                    "/") else uri.path
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

    def __repr__(self):
        return f"{self.protocol}://{self.hostname}:{self.port}/{self.id}"

    def rid(self):
        """Return the unique id of the component."""
        return self.id

    def isrepl(self):
        """Check if the URL is a REPL."""
        return self.protocol == "repl"

    def connection_url(self):
        """Return the URL string."""
        if self.hasname:
            return f"{self.protocol}://{self.hostname}:{self.port}/{self.id}"
        else:
            return f"{self.protocol}://{self.hostname}:{self.port}"


class Supervised:
    """
    A superclass that provides task supervision and auto-restarting for
    a designated task.
    Subclasses must implement the '_task_impl' coroutine.
    """

    def __init__(self):
        self._task: Optional[asyncio.Task[None]] = None
        self._supervisor_task: Optional[asyncio.Task[None]] = None
        self._should_run = True  # Flag to control supervisor loop

    async def _shutdown(self) -> None:
        """Override in subclasses for custom shutdown logic."""

    async def _task_impl(self) -> None:
        """Override in subclasses for supervised task impl."""

    async def _supervisor(self) -> None:
        """
        Supervises the _task_impl, restarting it if it exits
        unexpectedly or due to an exception.
        """
        while self._should_run:
            logger.debug("[%s] starting supervised task", self)
            self._task = asyncio.create_task(self._task_impl())
            try:
                await self._task
            except asyncio.CancelledError:
                logger.debug("[%s] task cancelled, exiting", self)
                self._should_run = False  # Ensure supervisor also stops
                break
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("[%s] error: %s (restarting)", self, e)
                logging.exception("traceback for task error:")
                if self._should_run:
                    await asyncio.sleep(0.5)

    def start(self) -> None:
        """Starts the supervisor task."""
        self._should_run = True
        self._supervisor_task = asyncio.create_task(self._supervisor())

    async def shutdown(self) -> None:
        """Gracefully stops the supervised worker and its supervisor."""
        logger.debug("[%s] shutting down", self)
        self._should_run = False

        await self._shutdown()

        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.debug("[%s] supervised task cancelled", self)

        if self._supervisor_task and not self._supervisor_task.done():
            self._supervisor_task.cancel()
            try:
                await self._supervisor_task
                logger.debug("[%s] supervisor task cancelled", self)
            except asyncio.CancelledError:
                pass
        logger.debug("[%s] shutdown complete", self)


class Router(Supervised):
    """
    A Router is a central component that manages connections and interactions
    between Rembus components(Twins).
    """

    def __init__(self, name: str):
        super().__init__()
        self.id = name
        self.id_twin: dict[str, Twin] = {}
        self.handler: dict[str, Callable[..., Any]] = {}
        self.inbox: asyncio.Queue[Any] = asyncio.Queue()
        self.shared: Any = None
        self.serve_task: Optional[asyncio.Task[None]] = None
        self.server_instance = None  # To store the server object
        self._shutdown_event = asyncio.Event()  # For controlled shutdown
        self.config = rs.Config(name)
        self.owners = rs.load_tenants(self)
        self.start_ts = time.time()
        self._builtins()
        self.start()

    def __str__(self):
        return f"{self.id}"

    def __repr__(self):
        return f"{self.id}: {self.id_twin}"

    def isconnected(self, rid: str) -> bool:
        """Check if a component with the given rid is connected."""
        return rid in self.id_twin

    def uptime(self) -> str:
        """Return the uptime of the router."""
        return f"up for {int(time.time() - self.start_ts)} seconds"

    def _builtins(self):
        self.handler["rid"] = lambda: self.id
        self.handler["version"] = lambda: __version__
        self.handler["uptime"] = self.uptime

    async def _shutdown(self):
        """Cleanup logic when shutting down the router."""
        logger.debug("[%s] router shutdown", self)
        if self.server_instance:
            self.server_instance.close()
            self._shutdown_event.set()
            await self.server_instance.wait_closed()

    async def _task_impl(self) -> None:
        logger.debug("[%s] router started", self)
        while True:
            msg = await self.inbox.get()
            if isinstance(msg, rp.IdentityMsg):
                twin_id = msg.cid
                sts = rp.STS_OK
                if self.isconnected(twin_id):
                    sts = rp.STS_ERROR
                    logger.warning(
                        "[%s] node with id [%s] is already connected",
                        self,
                        twin_id
                    )
                    await msg.twin.close()
                else:
                    logger.debug("[%s] identity: %s", self, msg.cid)
                    await self._auth_identity(msg)
            elif isinstance(msg, rp.AttestationMsg):
                sts = self._verify_signature(msg)
                await msg.twin.response(sts, msg)
            elif isinstance(msg, rp.AdminMsg):
                logger.debug("[%s] admin: %s", self, msg)
                await msg.twin.response(rp.STS_OK, msg)
            elif isinstance(msg, rp.RegisterMsg):
                logger.debug("[%s] register: %s", self, msg)
                await self._register_node(msg)
            elif isinstance(msg, rp.UnregisterMsg):
                logger.debug("[%s] unregister: %s", self, msg)
                await self._unregister_node(msg)

    async def evaluate(self, twin, topic: str, data: Any) -> Any:
        """Invoke the handler associate with the message topic."""
        if self.shared is not None:
            output = await get_response(
                self.handler[topic](self.shared, twin, *getargs(data))
            )
        else:
            output = await get_response(self.handler[topic](*getargs(data)))

        return output

    async def _client_receiver(self, ws):
        """Receive messages from the client component."""
        url = RbURL()
        rid = url.rid()
        twin = Twin(url, self, False)
        self.id_twin[rid] = twin
        twin.socket = ws
        await twin.twin_receiver()

    async def serve_ws(self, port: int, issecure: bool = False):
        """Start a WebSocket server to handle incoming connections."""
        ssl_context = None
        if issecure:
            trust_store = rs.keystore_dir()
            cert_path = os.path.join(trust_store, "rembus.crt")
            key_path = os.path.join(trust_store, "rembus.key")
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            if not os.path.isfile(cert_path) or not os.path.isfile(key_path):
                raise RuntimeError(f"SSL secrets not found in {trust_store}")

            ssl_context.load_cert_chain(cert_path, keyfile=key_path)

        async with serve(
                self._client_receiver,
                "0.0.0.0",
                port,
                ssl=ssl_context,
                ping_interval=self.config.ws_ping_interval,) as server:
            self.server_instance = server
            await self._shutdown_event.wait()
            # await server.serve_forever()

    def _needs_auth(self, cid: str):
        """Check if the component needs authentication."""
        try:
            rs.key_file(self.id, cid)
            return True
        except FileNotFoundError:
            return False

    def _update_twin(self, twin, identity):
        logger.debug("[%s] setting name: [%s]", twin, identity)
        self.id_twin[identity] = self.id_twin.pop(twin.rid, twin)
        twin.rid = identity

    def _verify_signature(self, msg: rp.AttestationMsg):
        """Verify the signature of the attestation message."""
        twin = msg.twin
        cid = msg.cid
        fn = twin.handler.pop("challenge")
        challenge = fn(twin)
        plain = cbor2.dumps([challenge, msg.cid])
        try:
            pubkey = rp.load_public_key(self, cid)
            if isinstance(pubkey, rsa.RSAPublicKey):
                pubkey.verify(msg.signature, plain,
                              padding.PKCS1v15(), hashes.SHA256())
            elif isinstance(pubkey, ec.EllipticCurvePublicKey):
                pubkey.verify(msg.signature, plain, ec.ECDSA(hashes.SHA256()))

            self._update_twin(twin, msg.cid)
            return rp.STS_OK
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("verification failed: %s (%s)", e, type(e))
            return rp.STS_ERROR

    def _challenge(self, msg: rp.IdentityMsg):
        """Generate a challenge for the identity authentication."""
        twin = msg.twin
        challenge_val = os.urandom(4)
        twin.handler["challenge"] = lambda twin: challenge_val
        return [rp.TYPE_RESPONSE, msg.id, rp.STS_CHALLENGE, challenge_val]

    async def _auth_identity(self, msg: rp.IdentityMsg):
        """Authenticate the identity of the component."""
        twin = msg.twin
        identity = msg.cid

        if self._needs_auth(identity):
            # component is provisioned, send the challenge
            response = self._challenge(msg)
        else:
            self._update_twin(twin, identity)
            response = [rp.TYPE_RESPONSE, msg.id, rp.STS_OK]

        # pylint: disable=protected-access
        await twin._send(cbor2.dumps(response))

    def _get_token(self, tenant, mid: bytes):
        """Get the token embedded into the message id."""
        vals = mid[3::-1]
        token = vals.hex()
        pin = self.owners.get(tenant)
        if token != pin:
            logger.info("tenant %s: invalid token %s", tenant, token)
            return None
        else:
            logger.debug("tenant %s: token is valid", tenant)
            return token

    async def _register_node(self, msg: rp.RegisterMsg):
        """Provision a new node."""
        sts = rp.STS_ERROR
        reason = None
        token = self._get_token(domain(msg.cid), msg.id)
        try:
            if token is None:
                reason = "wrong tenant/pin"
            elif rp.isregistered(self.id, msg.cid):
                sts = rp.STS_NAME_ALREADY_TAKEN
                reason = f"[{msg.cid}] not available"
            else:
                kdir = rs.keys_dir(self.id)
                os.makedirs(kdir, exist_ok=True)
                rp.save_pubkey(self.id, msg.cid, msg.pubkey, msg.type)
                sts = rp.STS_OK
                logger.debug("cid %s registered", msg.cid)
        finally:
            await msg.twin.response(sts, msg, reason)

    async def _unregister_node(self, msg: rp.UnregisterMsg):
        """Unprovisions the component."""
        sts = rp.STS_ERROR
        reason = None
        try:
            cid = msg.twin.rid
            rp.remove_pubkey(self, cid)
            sts = rp.STS_OK
        finally:
            await msg.twin.response(sts, msg, reason)


class Twin(Supervised):
    """
A Twin represents a Rembus component, either as a client or server.
It handles the connection, message sending and receiving, and provides methods
for RPC, pub/sub, and other commands interactions.
    """

    def __init__(self, uid: RbURL, router: Router, isclient: bool = True):
        super().__init__()
        self.isclient = isclient
        self._router = router
        self.socket: websockets.ClientConnection | None = None
        self.receiver = None
        self.uid = uid
        self.inbox: asyncio.Queue[str] = asyncio.Queue()
        self.handler: dict[str, Callable[..., Any]] = {}
        self.outreq: dict[bytes, FutureResponse] = {}
        self.reconnect_task: Optional[asyncio.Task[None]] = None
        self.ackdf: dict[int, int] = {}  # msgid => ts
        self.handler["phase"] = lambda: "CLOSED"
        self.msg_class = {
            rp.TYPE_UNREGISTER: rp.UnregisterMsg,
            rp.TYPE_REGISTER: rp.RegisterMsg,
            rp.TYPE_IDENTITY: rp.IdentityMsg,
            rp.TYPE_ATTESTATION: rp.AttestationMsg,
            rp.TYPE_ADMIN: rp.AdminMsg,
        }
        self.start()

    def __str__(self):
        return f"{self.uid.id}"

    def __repr__(self):
        return self.uid.id

    @property
    def rid(self):
        """Return the unique id of the rembus component."""
        return self.uid.id

    @rid.setter
    def rid(self, rid: str):
        self.uid.id = rid

    @property
    def router(self):
        """Return the router associated with this twin."""
        return self._router

    def isrepl(self) -> bool:
        """Check if twin is a REPL"""
        return self.uid.protocol == "repl"

    def isopen(self) -> bool:
        """Check if the connection is open."""
        return (self.socket is not None and
                self.socket.state == websockets.State.OPEN)

    async def response(self, status: int, msg: Any, data: Any = None):
        """Send a response to the client."""
        outmsg: Any = [rp.TYPE_RESPONSE, msg.id, status, data]
        await self._send(cbor2.dumps(outmsg))

    def inject(self, data: Any):
        """Initialize the context object."""
        self.router.shared = data

    async def _reconnect(self):
        logger.debug("[%s]: reconnecting ...", self)
        while True:
            try:
                await self.connect()
                await self.reactive()
                self.reconnect_task = None
                break
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.info("[%s] reconnect: %s", self, type(e))
                await asyncio.sleep(2)

    async def _shutdown(self):
        """Cleanup logic when shutting down the twin."""
        logger.debug("[%s] twin shutdown", self)
        if self.socket:
            await self.socket.close()
            self.socket = None

        if self.receiver:
            self.receiver.cancel()
            try:
                await self.receiver
            except asyncio.CancelledError:
                pass
            self.receiver = None

        if self.reconnect_task:
            self.reconnect_task.cancel()
            try:
                await self.reconnect_task
            except asyncio.CancelledError:
                pass
            self.reconnect_task = None

        if self.isclient or self.uid.isrepl():
            await self.router.shutdown()

    async def _task_impl(self):
        logger.debug("[%s] task started", self)
        while True:
            msg: str = await self.inbox.get()
            logger.debug("[%s] twin_task: %s", self, msg)
            if msg == "reconnect":
                if not self.reconnect_task:
                    self.reconnect_task = asyncio.create_task(
                        self._reconnect())

    async def twin_receiver(self):
        """Receive messages from the WebSocket connection."""
        logger.debug("[%s] client is connected", self)
        try:
            while self.socket is not None:
                result: str | bytes = await self.socket.recv()
                if isinstance(result, str):
                    raise rp.RembusError(
                        rp.STS_ERROR, "unexpected text message")
                msg: list[Any] = cbor2.loads(result)
                logger.debug("[%s] %s", self, rp.msg_str('in', msg))
                await self._parse_input(msg)
        except websockets.ConnectionClosedOK:
            logger.debug("connection closed")
        except websockets.ConnectionClosedError as e:
            logger.info("connection closed (%s): %s", e, type(e))
        finally:
            if self.isclient and self.handler["phase"]() == "CONNECTED":
                logger.debug("[%s] twin_receiver done", self)
                await self.inbox.put("reconnect")
            else:
                self.router.id_twin.pop(self.rid, None)
                await self.shutdown()

    async def future_request(self, msgid: bytes):
        """Return the future associated with the message id `msgid`."""
        fut = self.outreq.pop(msgid, None)
        if fut is None:
            logger.warning("[%s] recv unknown msg id %s",
                           self, rp.tohex(msgid))
        return fut

    async def _pubsub_msg(self, msg: list[Any], flags):
        if flags > rp.QOS0:
            mid = msg[1]
            topic: str = msg[2]
            data = msg[3]
            if self.socket:
                await self.socket.send(cbor2.dumps([rp.TYPE_ACK, mid]))
            if flags == rp.QOS2:
                id128 = rp.bytes2id(mid)
                if id128 in self.ackdf:
                    # Already received, skip the message.
                    return
                else:
                    # Save the message id to guarantee exactly one delivery.
                    self.ackdf[id128] = int(time.time())
        else:
            topic: str = msg[1]
            data = rp.tag2df(msg[2])
        try:
            if topic in self.router.handler:
                await self.router.evaluate(self, topic, data)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("[%s] error in method invocation: %s", self, e)

        return

    async def _rpcreq_msg(self, msg: list[Any]):
        """Handle an RPC request."""
        mid = msg[1]
        topic = msg[2]
        data = rp.tag2df(msg[4])
        if topic not in self.router.handler:
            outmsg = [rp.TYPE_RESPONSE, mid, rp.STS_METHOD_NOT_FOUND, topic]
        else:
            status = rp.STS_OK
            try:
                output = await self.router.evaluate(self, topic, data)
            except Exception as e:  # pylint: disable=broad-exception-caught
                status = rp.STS_METHOD_EXCEPTION
                output = f"{e}"
                logger.debug("exception: %s", e)
            outmsg: Any = [rp.TYPE_RESPONSE, mid, status, rp.df2tag(output)]
            logger.debug("[%s] %s", self, rp.msg_str('out', outmsg))
        await self._send(cbor2.dumps(outmsg))

        return

    async def _response_msg(self, msg: list[Any]):
        mid = msg[1]
        fut = await self.future_request(mid)
        if fut:
            sts = msg[2]
            payload = (msg[3:] + [None])[0]
            if sts == rp.STS_OK:
                fut.future.set_result(rp.tag2df(payload))
            elif sts == rp.STS_CHALLENGE:
                fut.future.set_result(payload)
            else:
                fut.future.set_exception(rp.RembusError(sts, payload))

    async def _ack_msg(self, msg: list[Any]):
        mid = msg[1]
        logger.debug("pubsub ack data")
        fut = await self.future_request(mid)
        if fut:
            if fut.data:
                # fut.data is true if the message is a QOS2
                await self._send(rp.encode([rp.TYPE_ACK2, mid]))
            fut.future.set_result(True)

    async def _ack2_msg(self, msg: list[Any]):
        mid = rp.bytes2id(msg[1])
        if mid in self.ackdf:
            logger.debug("deleting pubsub ack: %s", mid)
            del self.ackdf[mid]
        return

    async def _parse_input(self, msg: list[Any]):
        """Receive the incoming message and dispatch it to the appropriate handler."""
        type_byte = msg[0]

        mtype = type_byte & 0x0F
        flags = type_byte & 0xF0
        if mtype == rp.TYPE_PUB:
            await self._pubsub_msg(msg, flags)
        elif mtype == rp.TYPE_RPC:
            await self._rpcreq_msg(msg)
        elif mtype == rp.TYPE_RESPONSE:
            await self._response_msg(msg)
        elif mtype == rp.TYPE_ACK:
            await self._ack_msg(msg)
        elif mtype == rp.TYPE_ACK2:
            await self._ack2_msg(msg)
        elif mtype in self.msg_class:
            msg_class = self.msg_class[mtype]
            self.router.inbox.put_nowait(msg_class(self, msg))

    async def connect(self):
        """Connect to the broker."""
        broker_url = self.uid.connection_url()
        ssl_context = None
        if self.uid.protocol == "wss":
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ca_crt = os.getenv("HTTP_CA_BUNDLE", rs.rembus_ca())
            if os.path.isfile(ca_crt):
                ssl_context.load_verify_locations(ca_crt)
            else:
                logger.warning("CA file not found: %s", ca_crt)

        self.socket = await websockets.connect(
            broker_url,
            ping_interval=self.router.config.ws_ping_interval,
            max_size=rp.WS_FRAME_MAXSIZE,
            ssl=ssl_context
        )
        self.handler["phase"] = lambda: "CONNECTING"
        self.receiver = asyncio.create_task(self.twin_receiver())

        if self.uid.hasname:
            try:
                await self._login()
            except Exception as e:
                await self.close()
                self.handler["phase"] = lambda: "CLOSED"
                raise rp.RembusError(rp.STS_ERROR, "_login failed") from e

        self.handler["phase"] = lambda: "CONNECTED"
        return self

    async def _send(self, payload: bytes) -> Any:
        if self.socket is None:
            raise rp.RembusConnectionClosed()

        await self.socket.send(payload)

    async def _send_wait(self, builder: Callable[[bytearray], bytes], data: Any = None) -> Any:
        """Send a message and wait for a response."""
        reqid = rp.msgid()
        req = builder(reqid)
        kid = bytes(reqid)
        await self._send(req)
        futreq = FutureResponse(data)
        self.outreq[kid] = futreq
        try:
            async with async_timeout.timeout(
                self.router.config.request_timeout
            ):
                return await futreq.future
        except TimeoutError as e:
            raise rp.RembusTimeout() from e

    async def _login(self):
        """Connect in free mode or authenticate the provisioned component."""
        challenge = await self._send_wait(
            lambda id: rp.encode([rp.TYPE_IDENTITY, id, self.uid.id])
        )
        if challenge and isinstance(challenge, bytes):
            plain = [bytes(challenge), self.uid.id]
            message = cbor2.dumps(plain)
            privatekey = rp.load_private_key(self.uid.id)
            if isinstance(privatekey, rsa.RSAPrivateKey):
                signature: bytes = privatekey.sign(
                    message, padding.PKCS1v15(), hashes.SHA256())
            elif isinstance(privatekey, ec.EllipticCurvePrivateKey):
                signature: bytes = privatekey.sign(
                    message, ec.ECDSA(hashes.SHA256()))

            await self._send_wait(
                lambda id: rp.encode(
                    [rp.TYPE_ATTESTATION, id, self.uid.id, signature])
            )
        else:
            logger.debug("[%s]: free mode access", self)

    async def publish(self, topic: str, *args: tuple[Any], **kwargs):
        """Publish a message to the specified topic."""
        data = rp.df2tag(args)
        if self.socket is None:
            raise rp.RembusConnectionClosed()
        qos = kwargs.get("qos", rp.QOS0)

        if qos == rp.QOS0:
            await self.socket.send(rp.encode([rp.TYPE_PUB | qos, topic, data]))
        else:
            done = False
            while not done:
                try:
                    done = await self._send_wait(
                        lambda id: rp.encode(
                            [rp.TYPE_PUB | qos, id, topic, data]),
                        qos == rp.QOS2
                    )
                except rp.RembusTimeout:
                    pass

        return None

    async def broker_setting(self, command: str, args: dict[str, Any]):
        """Send a broker configuration command."""
        data = {rp.COMMAND: command} | args
        return await self._send_wait(
            lambda id: rp.encode([rp.TYPE_ADMIN, id, rp.BROKER_CONFIG, data])
        )

    async def setting(
            self, topic: str, command: str, args: dict[str, Any] | None = None
    ):
        """Send an admin command to the broker."""
        if self.socket:
            if args:
                data = {rp.COMMAND: command} | args
            else:
                data = {rp.COMMAND: command}

            return await self._send_wait(
                lambda id: rp.encode([rp.TYPE_ADMIN, id, topic, data])
            )

    async def rpc(self, topic: str, *args: tuple[Any]):
        """Send a RPC request."""
        data = rp.df2tag(args)
        return await self._send_wait(
            lambda id: rp.encode([rp.TYPE_RPC, id, topic, None, data])
        )

    async def direct(self, target: str, topic: str, *args: tuple[Any]):
        """Send a RPC request to a specific target."""
        data = rp.df2tag(args)
        return await self._send_wait(
            lambda id: rp.encode([rp.TYPE_RPC, id, topic, target, data])
        )

    async def register(self, rid: str, pin: str, scheme: int = rp.SIG_RSA):
        """Provisions the component with rid identifier."""
        if scheme == rp.SIG_RSA:
            privkey = rp.rsa_private_key()
        else:
            privkey = rp.ecdsa_private_key()

        pubkey = rp.pem_public_key(privkey)
        response = await self._send_wait(
            lambda id: rp.encode(
                [rp.TYPE_REGISTER, rp.regid(id, pin), rid, pubkey, scheme]
            ))

        logger.debug("cid %s registered", rid)
        rp.save_private_key(rid, privkey)
        return response

    async def unregister(self):
        """Unprovisions the component."""
        await self._send_wait(
            lambda id: rp.encode([rp.TYPE_UNREGISTER, id, self.uid.id])
        )
        os.remove(os.path.join(rs.rembus_dir(), self.uid.id, ".secret"))
        return self

    async def reactive(self):
        """
        Set the component to receive published messages on subscribed topics.
        """
        await self.broker_setting("reactive", {"status": True})
        return self

    async def unreactive(self):
        """
        Set the component to stop receiving published messages on subscribed topics.
        """
        await self.broker_setting("reactive", {"status": False})
        return self

    async def subscribe(
            self, fn: Callable[..., Any], retroactive: bool = False
    ):
        """
        Subscribe the function to the corresponding topic.
        """
        topic = fn.__name__
        await self.setting(
            topic, rp.ADD_INTEREST, {"retroactive": retroactive}
        )
        self.router.handler[topic] = fn
        return self

    async def unsubscribe(self, fn: Callable[..., Any]):
        """
        Unsubscribe the function from the corresponding topic.
        """
        topic = fn.__name__
        await self.setting(topic, rp.REMOVE_INTEREST)
        self.router.handler.pop(topic, None)
        return self

    async def expose(self, fn: Callable[..., Any]):
        """
        Expose the function as a remote procedure call(RPC) handler.
        """
        topic = fn.__name__
        self.router.handler[topic] = fn
        await self.setting(topic, rp.ADD_IMPL)

    async def unexpose(self, fn: Callable[..., Any]):
        """
        Unexpose the function as a remote procedure call(RPC) handler.
        """
        topic = fn.__name__
        self.router.handler.pop(topic, None)
        await self.setting(topic, rp.REMOVE_IMPL)

    async def close(self):
        """Close the connection and clean up resources."""
        await self.shutdown()

    async def wait(self, timeout: float | None = None):
        """
        Start the twin event loop that wait for rembus messages.
        """
        if not self.isrepl():
            await self.reactive()
        if self._supervisor_task is not None:
            return await asyncio.wait([self._supervisor_task], timeout=timeout)


async def component(
    url: str | None = None,
    name: str | None = None,
    port: int | None = None,
    secure: bool = False
) -> Twin:
    """Return a Rembus component."""
    isserver = (port is not None) and (url is None)
    if url:
        uid = RbURL(url)
    else:
        uid = RbURL("repl://") if isserver else RbURL()
    router_name = name if name else rs.DEFAULT_BROKER
    router = Router(router_name)
    logger.debug("component %s created, port: %s", uid.id, port)
    # start a websocket server
    if port:
        router.serve_task = asyncio.create_task(router.serve_ws(port, secure))
        done, _ = await asyncio.wait([router.serve_task], timeout=0.1)
        if router.serve_task in done:
            try:
                await router.serve_task
            except Exception as e:
                router.serve_task = None
                logger.error("[%s] start failed: %s", router, e)
                raise
    else:
        router.serve_task = None

    cmp = Twin(uid, router, not isserver)
    try:
        if not isserver:
            await cmp.connect()
    except Exception:
        await cmp.close()
        raise
    return cmp
