"""
The core module of the Rembus library that includes implementations for the
Router and Twin concepts.
"""
import asyncio
import base64
import logging
import os
import time
from typing import Callable, Any, Optional, List
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


def bytes_to_b64(val: bytes, enc: int):
    """Base 64 encodeing for JSON-RPC transport"""
    if enc == rp.JSON:
        return base64.b64encode(val).decode("utf-8")
    return val


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
        self.future = asyncio.get_running_loop().create_future()
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
        default_url = os.getenv('REMBUS_BASE_URL', "ws://127.0.0.1:8000")
        baseurl = urlparse(default_url)
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

    def isrepl(self):
        """Check if the URL is a REPL."""
        return self.protocol == "repl"

    def connection_url(self):
        """Return the URL string."""
        if self.hasname:
            return f"{self.protocol}://{self.hostname}:{self.port}/{self.id}"
        else:
            return f"{self.protocol}://{self.hostname}:{self.port}"

    @property
    def netlink(self):
        """Return the remote connection endpoint"""
        return f"{self.protocol}://{self.hostname}:{self.port}"

    @property
    def twkey(self):
        """Return the twin key"""
        return f"{self.id}@{self.netlink}"


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


async def init_router(router_name, uid, port, secure):
    """Start the router"""
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
                await router.shutdown()
                raise
    else:
        router.serve_task = None

    return router


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
        for tk in self.id_twin:
            if tk.startswith(rid+'@'):
                return True
        return False

    def uptime(self) -> str:
        """Return the uptime of the router."""
        return f"up for {int(time.time() - self.start_ts)} seconds"

    def _builtins(self):
        self.handler["rid"] = lambda *_: self.id
        self.handler["version"] = lambda *_: __version__
        self.handler["uptime"] = lambda *_: self.uptime()

    async def init_twin(self, uid: RbURL, enc: int, isserver: bool):
        """Create and start a Twin"""
        cmp = Twin(uid, self, not isserver, enc)
        if not uid.isrepl():
            self.id_twin[uid.twkey] = cmp

        try:
            # if not isserver:
            if not cmp.isrepl():
                if self.config.start_anyway:
                    await cmp.inbox.put("reconnect")
                else:
                    await cmp.connect()
        except Exception:
            await cmp.close()
            raise
        return cmp

    async def _shutdown(self):
        """Cleanup logic when shutting down the router."""
        logger.debug("[%s] router shutdown", self)
        if self.server_instance:
            self.server_instance.close()
            self._shutdown_event.set()
            await self.server_instance.wait_closed()

    async def _pubsub_msg(self, msg: rp.PubSubMsg):
        twin = msg.twin
        if msg.flags > rp.QOSLevel.QOS0 and msg.id:
            if twin.socket:
                await twin.send(rp.AckMsg(id=msg.id))

            if msg.flags == rp.QOSLevel.QOS2:
                if msg.id in twin.ackdf:
                    # Already received, skip the message.
                    return
                else:
                    # Save the message id to guarantee exactly one delivery.
                    twin.ackdf[msg.id] = int(time.time())

        data = rp.tag2df(msg.data)
        try:
            if msg.topic in self.handler:
                await self.evaluate(self, msg.topic, data)

            for t in self.id_twin.values():
                if t != twin:
                    # Do not send back to publisher.
                    await t.send(msg)

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("[%s] error in method invocation: %s", self, e)

        return

    async def send_message(self, msg):
        """Send message to remote node using a twin from the pool of twins"""
        for t in self.id_twin.values():
            if t.isopen():
                try:
                    sts = rp.STS_OK
                    data = await t.msg_send_wait(msg)
                except Exception as e:  # pylint:disable=broad-exception-caught
                    sts = rp.STS_ERROR
                    data = f"{e}"
                fut = await msg.twin.future_request(msg.id)
                if fut:
                    if sts == rp.STS_OK:
                        fut.future.set_result(rp.tag2df(data))
                    else:
                        fut.future.set_exception(
                            rp.RembusError(rp.STS_ERROR, data))
                break

    async def _rpcreq_msg(self, msg: rp.RpcReqMsg):
        """Handle an RPC request."""
        data = rp.tag2df(msg.data)

        if msg.twin.isrepl():
            await self.send_message(msg)
        elif msg.topic in self.handler:
            status = rp.STS_OK
            try:
                output = await self.evaluate(self, msg.topic, data)
            except Exception as e:  # pylint: disable=broad-exception-caught
                status = rp.STS_METHOD_EXCEPTION
                output = f"{e}"
                logger.debug("exception: %s", e)
            outmsg = rp.ResMsg(
                id=msg.id, status=status, data=rp.df2tag(output)
            )
            await msg.twin.send(outmsg)
        else:
            outmsg = rp.ResMsg(
                id=msg.id, status=rp.STS_METHOD_NOT_FOUND, data=msg.topic
            )
            await msg.twin.send(outmsg)
        return

    async def _task_impl(self) -> None:
        logger.debug("[%s] router started", self)
        while True:
            msg = await self.inbox.get()
            if isinstance(msg, rp.PubSubMsg):
                await self._pubsub_msg(msg)
            elif isinstance(msg, rp.RpcReqMsg):
                await self._rpcreq_msg(msg)
            elif isinstance(msg, rp.IdentityMsg):
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
                if msg.twin.isrepl():
                    await self.send_message(msg)
                else:
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
        twin = Twin(url, self, False)
        self.id_twin[url.twkey] = twin
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
        self.id_twin.pop(twin.twkey, twin)
        twin.rid = identity
        self.id_twin[twin.twkey] = twin

    def _verify_signature(self, msg: rp.AttestationMsg):
        """Verify the signature of the attestation message."""
        twin = msg.twin
        cid = msg.cid
        fn = twin.handler.pop("challenge")
        challenge = fn(twin)
        plain = cbor2.dumps([challenge, msg.cid])
        try:
            if isinstance(msg.signature, str):
                signature = base64.b64decode(msg.signature)
            else:
                signature = msg.signature

            pubkey = rp.load_public_key(self, cid)
            if isinstance(pubkey, rsa.RSAPublicKey):
                pubkey.verify(signature, plain,
                              padding.PKCS1v15(), hashes.SHA256())
            elif isinstance(pubkey, ec.EllipticCurvePublicKey):
                pubkey.verify(signature, plain, ec.ECDSA(hashes.SHA256()))

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
        return rp.ResMsg(
            id=msg.id,
            status=rp.STS_CHALLENGE,
            data=bytes_to_b64(challenge_val, twin.enc)
        )

    async def _auth_identity(self, msg: rp.IdentityMsg):
        """Authenticate the identity of the component."""
        twin = msg.twin
        identity = msg.cid

        if self._needs_auth(identity):
            # component is provisioned, send the challenge
            response = self._challenge(msg)
        else:
            self._update_twin(twin, identity)
            response = rp.ResMsg(id=msg.id, status=rp.STS_OK)

        await twin.send(response)

    def _get_token(self, tenant, secret: str):
        """Get the token embedded into the message id."""
        pin = self.owners.get(tenant)
        if secret != pin:
            logger.info("tenant %s: invalid token %s", tenant, secret)
            return None
        else:
            logger.debug("tenant %s: token is valid", tenant)
            return secret

    async def _register_node(self, msg: rp.RegisterMsg):
        """Provision a new node."""
        sts = rp.STS_ERROR
        reason = None
        token = self._get_token(domain(msg.cid), msg.pin)
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

    def __init__(
            self,
            uid: RbURL,
            router: Router,
            isclient: bool = True,
            enc: int = rp.CBOR):
        super().__init__()
        self.isclient = isclient
        self.enc = enc
        self._router = router
        self.socket: websockets.ClientConnection | None = None
        self.receiver = None
        self.uid = uid
        self.inbox: asyncio.Queue[str] = asyncio.Queue()
        self.handler: dict[str, Callable[..., Any]] = {}
        self.outreq: dict[int, FutureResponse] = {}
        self.reconnect_task: Optional[asyncio.Task[None]] = None
        self.ackdf: dict[int, int] = {}  # msgid => ts
        self.handler["phase"] = lambda: "CLOSED"
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
    def twkey(self):
        """Return the twin key"""
        return f"{self.uid.twkey}"

    @property
    def router(self):
        """Return the router associated with this twin."""
        return self._router

    def isrepl(self) -> bool:
        """Check if twin is a REPL"""
        return self.uid.protocol == "repl"

    def isopen(self) -> bool:
        """Check if the connection is open."""
        if self.isrepl():
            return any([t.isopen() for t in self.router.id_twin.values()])
        else:
            return (self.socket is not None and
                    self.socket.state == websockets.State.OPEN)

    async def response(self, status: int, msg: Any, data: Any = None):
        """Send a response to the client."""
        outmsg: Any = rp.ResMsg(id=msg.id, status=status, data=data)
        await self.send(outmsg)

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
                logger.info("[%s] reconnect: %s", self, e)
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

        if self.uid.isrepl():
            # close the twins (or the component of the pool)
            for (_, t) in list(self.router.id_twin.items()):
                t.handler["phase"] = lambda: "CLOSED"
                if t.socket is not None:
                    await t.socket.close()

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
                    self.enc = rp.JSON
                    msg = rp.jsonrpc_parse(result)
                else:
                    self.enc = rp.CBOR
                    pkt: list[Any] = cbor2.loads(result)
                    msg = rp.cbor_parse(pkt)

                msg.twin = self
                await self._eval_input(msg)
        except (
            websockets.ConnectionClosedOK,
            websockets.ConnectionClosedError
        ) as e:
            logger.debug("connection closed: %s", e)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("[%s] error: %s", self, e)
        finally:
            if self.isclient and self.handler["phase"]() == "CONNECTED":
                logger.debug("[%s] twin_receiver done", self)
                await self.inbox.put("reconnect")
            else:
                self.router.id_twin.pop(self.twkey, None)
                await self.shutdown()

    async def future_request(self, msgid: int):
        """Return the future associated with the message id `msgid`."""
        fut = self.outreq.pop(msgid, None)
        if fut is None:
            logger.warning("[%s] recv unknown msg id %s",
                           self, rp.tohex(rp.to_bytes(msgid)))
        elif fut.future.done():
            return None

        return fut

    async def _response_msg(self, msg: rp.ResMsg):
        fut = await self.future_request(msg.id)
        if fut:
            sts = msg.status
            if sts == rp.STS_OK:
                fut.future.set_result(rp.tag2df(msg.data))
            elif sts == rp.STS_CHALLENGE:
                fut.future.set_result(msg.data)
            else:
                fut.future.set_exception(rp.RembusError(sts, msg.data))

    async def _ack_msg(self, msg: rp.AckMsg):
        logger.debug("pubsub ack data")
        fut = await self.future_request(msg.id)
        if fut:
            if fut.data:
                await self.send(rp.Ack2Msg(id=msg.id))

            fut.future.set_result(True)

    async def _ack2_msg(self, msg: rp.Ack2Msg):
        mid = msg.id
        if mid in self.ackdf:
            logger.debug("deleting pubsub ack: %s", mid)
            del self.ackdf[mid]
        return

    async def _eval_input(self, msg: rp.RembusMsg):
        """
        Receive the incoming message and dispatch
        it to the appropriate handler.
        """

        if isinstance(msg, rp.ResMsg):
            await self._response_msg(msg)
        elif isinstance(msg, rp.AckMsg):
            await self._ack_msg(msg)
        elif isinstance(msg, rp.Ack2Msg):
            await self._ack2_msg(msg)
        else:
            self.router.inbox.put_nowait(msg)

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

    async def send(self, msg: rp.RembusMsg):
        """Send a rembus message"""
        pkt = msg.to_payload(self.enc)
        if self.isrepl() and self.isopen():
            await self.router.inbox.put(msg)
        elif self.socket is None:
            raise rp.RembusConnectionClosed()
        else:
            pkt = msg.to_payload(self.enc)
            await self._send(pkt)

    async def _send(self, payload: bytes | str) -> Any:
        if self.socket is not None:
            await self.socket.send(payload)

    async def _send_wait(
            self,
            builder: Callable,
            data: Any = None) -> Any:
        """Send a message and wait for a response."""
        reqid = rp.msgid()
        req = builder(reqid)
        req.twin = self
        await self.send(req)
        futreq = FutureResponse(data)
        self.outreq[reqid] = futreq
        try:
            async with async_timeout.timeout(
                self.router.config.request_timeout
            ):
                return await futreq.future
        except TimeoutError as e:
            raise rp.RembusTimeout() from e

    async def msg_send_wait(
            self,
            msg: rp.RpcReqMsg) -> Any:
        """Send a message and wait for a response."""
        await self.send(msg)
        futreq = FutureResponse(msg.data)
        self.outreq[msg.id] = futreq
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
            lambda id: rp.IdentityMsg(id=id, cid=self.uid.id)
        )
        if isinstance(challenge, bytes | str):
            if isinstance(challenge, str):
                challenge = base64.b64decode(challenge)

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
                lambda id: rp.AttestationMsg(
                    id=id,
                    cid=self.uid.id,
                    signature=bytes_to_b64(signature, self.enc)
                ))
        else:
            logger.debug("[%s]: free mode access", self)

    async def publish(self, topic: str, *data: Any, **kwargs):
        """Publish a message to the specified topic."""
        qos = kwargs.get("qos", rp.QOSLevel.QOS0)
        if qos == rp.QOSLevel.QOS0:
            msg = rp.PubSubMsg(topic=topic, data=data)
            msg.twin = self
            if self.isrepl() and self.isopen():
                await self.router.inbox.put(msg)
            elif self.socket is None:
                raise rp.RembusConnectionClosed()
            else:
                await self.send(msg)
        else:
            await self._qos_publish(topic, data, qos)

        return None

    async def put(self, topic: str, *args: Any, **kwargs):
        """Publish a message to the topic prefixed with component name."""
        await self.publish(self.rid + '/' + topic, *args, **kwargs)

    async def _qos_publish(
            self,
            topic: str,
            data: tuple,
            qos: rp.QOSLevel
    ):
        done = False
        max_retries = self.router.config.send_retries
        retries = 0
        while True:
            retries += 1
            try:
                done = await self._send_wait(
                    lambda id: rp.PubSubMsg(
                        id=id, topic=topic, data=data, flags=qos),
                    qos == rp.QOSLevel.QOS2
                )
                if done:
                    break
            except rp.RembusTimeout:
                if retries > max_retries:
                    raise rp.RembusTimeout() from None

    async def broker_setting(self, command: str, args: dict[str, Any]):
        """Send a broker configuration command."""
        data = {rp.COMMAND: command} | args
        return await self._send_wait(
            lambda id: rp.AdminMsg(id=id, topic=rp.BROKER_CONFIG, data=data)
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
                lambda id: rp.AdminMsg(id=id, topic=topic, data=data)
            )

    async def rpc(self, topic: str, *args: Any):
        """Send a RPC request."""
        data = rp.df2tag(args)
        return await self._send_wait(
            lambda id: rp.RpcReqMsg(id=id, topic=topic, data=data)
        )

    async def direct(self, target: str, topic: str, *args: Any):
        """Send a RPC request to a specific target."""
        data = rp.df2tag(args)
        return await self._send_wait(
            lambda id: rp.RpcReqMsg(
                id=id, topic=topic, target=target, data=data)
        )

    async def register(self, rid: str, pin: str, scheme: int = rp.SIG_RSA):
        """Provisions the component with rid identifier."""
        if scheme == rp.SIG_RSA:
            privkey = rp.rsa_private_key()
        else:
            privkey = rp.ecdsa_private_key()

        pubkey = rp.pem_public_key(privkey)
        if self.enc == rp.JSON:
            pubkey = base64.b64encode(pubkey).decode("utf-8")

        response = await self._send_wait(
            lambda id: rp.RegisterMsg(
                id=id, cid=rid, pin=pin, pubkey=pubkey, type=scheme)
        )

        logger.debug("cid %s registered", rid)
        rp.save_private_key(rid, privkey)
        return response

    async def unregister(self):
        """Unprovisions the component."""
        await self._send_wait(
            lambda id: rp.UnregisterMsg(id=id)
        )
        os.remove(os.path.join(rs.rembus_dir(), self.uid.id, ".secret"))
        return self

    async def reactive(self):
        """
        Set the component to receive published messages on subscribed topics.
        """
        if self.isclient:
            await self.broker_setting("reactive", {"status": True})
        return self

    async def unreactive(self):
        """
        Set the component to stop receiving published
        messages on subscribed topics.
        """
        await self.broker_setting("reactive", {"status": False})
        return self

    async def subscribe(
            self,
            fn: Callable[..., Any],
            retroactive: bool = False,
            topic: Optional[str] = None
    ):
        """
        Subscribe the function to the corresponding topic.
        """
        if topic is None:
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
            try:
                await asyncio.wait([self._supervisor_task], timeout=timeout)
            except asyncio.exceptions.CancelledError:
                pass
            finally:
                await self.shutdown()


async def component(
    url: str | List[str] | None = None,
    name: str | None = None,
    port: int | None = None,
    secure: bool = False,
    enc: int = rp.CBOR
) -> Twin:
    """Return a Rembus component."""
    isserver = (port is not None) and (url is None)
    if isinstance(url, str):
        uid = RbURL(url)
    else:
        uid = RbURL("repl://")

    default_name = rs.DEFAULT_BROKER
    if uid.hasname:
        default_name = uid.id

    router_name = name if name else default_name
    router = await init_router(router_name, uid, port, secure)

    handle = await router.init_twin(uid, enc, isserver)
    if isinstance(url, list):
        for netlink in url:
            await router.init_twin(RbURL(netlink), enc, isserver)

    return handle
