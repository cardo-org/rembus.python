"""
The core module of the Rembus library that includes implementations for the
Router and Twin concepts.
"""

from __future__ import annotations
import asyncio
from contextlib import suppress
import base64
from enum import Enum
from functools import partial
import json
import logging
import os
import time
import traceback
from typing import Callable, Any, Optional, List, cast
import signal
import ssl
from urllib.parse import urlparse
import uuid
import async_timeout
from websockets.asyncio.server import serve
import cbor2
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa, ec
import polars as pl
import websockets
from gmqtt import Client as MQTTClient
import rembus.protocol as rp
import rembus.settings as rs
import rembus.db as rdb
import rembus.builtins as builtins
from . import __version__
from .admin import admin_command

logger = logging.getLogger(__name__)


def get_ssl_context():
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ca_crt = os.getenv("HTTP_CA_BUNDLE", rs.rembus_ca())
    if os.path.isfile(ca_crt):
        ssl_context.load_verify_locations(ca_crt)
    else:
        logger.warning("CA file not found: %s", ca_crt)
    return ssl_context


class Policy(Enum):
    """Load balancing policies for selecting twins."""

    FIRST_UP = "first_up"
    ROUND_ROBIN = "round_robin"
    LESS_BUSY = "less_busy"


def less_busy(tenant, implementors):
    """Select the twin with fewer outstanding requests."""
    up_and_running = [
        t for t in implementors if t.isopen() and t.domain == tenant
    ]

    if not up_and_running:
        return None

    return min(up_and_running)


def round_robin(router, tenant, topic, implementors):
    """Select the next twin in a round-robin fashion."""
    if not implementors:
        return None

    length = len(implementors)

    current_index = router.last_invoked.get(topic, 0)
    target = None
    candidate = None
    candidate_index = 0

    # Iterate with 1-based indexing to mirror Julia `enumerate`
    for ix, tw in enumerate(implementors):
        if ix < current_index:
            # candidate for wrap-around
            if candidate is None and tw.isopen() and tw.domain == tenant:
                candidate = tw
                candidate_index = ix

        else:
            # forward search from current_index
            if tw.isopen() and tw.domain == tenant:
                target = tw
                router.last_invoked[topic] = 0 if ix >= length - 1 else ix + 1
                break

    # If no forward match found but we have a candidate
    if target is None and candidate is not None:
        target = candidate
        router.last_invoked[topic] = (
            0 if candidate_index >= length - 1 else candidate_index + 1
        )

    return target


def domain(s: str) -> str:
    """Return the domain part from the string.

    If no domain is found, return the root domain ".".
    """
    dot_index = s.find(".")
    if dot_index != -1:
        return s[dot_index + 1 :]
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

    def __init__(self, task: asyncio.Task | None, data: Any = None):
        self.future = asyncio.get_running_loop().create_future()
        self.task = task
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
    It supports the 'repl' scheme, the standard 'ws'/'wss' and
    'mqtt/mqtts' schemes.
    """

    def __init__(self, url: str | None = None) -> None:
        default_url = os.getenv("REMBUS_BASE_URL", "ws://127.0.0.1:8000")
        baseurl = urlparse(default_url)
        u = urlparse(url)

        if u.scheme == "repl":
            self.protocol = u.scheme
            self.hostname = ""
            self.port = 0
            self.hasname = False
            self.id = "repl"
        else:
            if isinstance(u.path, str) and u.path and u.path != "__noname__":
                self.hasname = True
                self.id = u.path[1:] if u.path.startswith("/") else u.path
            else:
                self.hasname = False
                self.id = randname()

            if u.scheme:
                self.protocol = u.scheme
            else:
                self.protocol = baseurl.scheme

            if u.hostname:
                self.hostname = u.hostname
            else:
                self.hostname = baseurl.hostname

            if u.port:
                self.port = u.port
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
        return self.id if self.id == "repl" else f"{self.id}@{self.netlink}"


async def shutdown_message(obj) -> None:
    """Log a shutdown message for the given object."""
    logger.debug("[%s] sending shutdown message", obj)
    await obj.inbox.put("shutdown")


class Supervised:
    """
    A superclass that provides task supervision and auto-restarting for
    a designated task.
    Subclasses must implement the '_task_impl' coroutine.
    """

    downstream: Supervised | None
    upstream: Supervised | None

    def __init__(self):
        self.upstream = None
        self.downstream = None
        self._task: Optional[asyncio.Task[None]] = None
        self._supervisor_task: Optional[asyncio.Task[None]] = None
        self.inbox: asyncio.Queue[Any] = asyncio.Queue()
        self._should_run = True  # Flag to control supervisor loop

    async def _shutdown(self) -> None:
        """Override in subclasses for custom shutdown logic."""

    async def _task_impl(self) -> None:
        """Override in subclasses for supervised task impl."""

    async def _supervisor(self) -> None:
        """
        Supervises the _task_impl, restarting if it exits
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

    async def start(self) -> None:
        """Starts the supervisor task."""
        self._should_run = True
        self._supervisor_task = asyncio.create_task(self._supervisor())

    async def shutdown(self) -> None:
        """Gracefully stops the supervised worker and its supervisor."""
        logger.debug(
            "[%s] shutting down (should_run: %s)", self, self._should_run
        )
        if self._should_run:
            self._should_run = False

            await self._shutdown()

            if self._task and not self._task.done():
                await shutdown_message(self)
                await self._task

            if self._supervisor_task and not self._supervisor_task.done():
                self._supervisor_task.cancel()
                try:
                    await self._supervisor_task
                except asyncio.CancelledError:
                    pass
            logger.debug("[%s] shutdown complete", self)


async def init_router(router_name, policy, uid, port, secure, isserver, schema):
    """Start the router"""

    if policy not in ["first_up", "less_busy", "round_robin"]:
        raise ValueError(
            "wrong routing policy, must be one of first_up, "
            "less_busy, round_robin"
        )

    router = Router(router_name, policy, isserver, schema)
    await router.start()
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

    def __init__(
        self,
        name: str,
        policy: str = "first_up",
        data_at_rest: bool = True,
        schema: str | None = None,
    ):
        super().__init__()
        self.id = name
        self.id_twin: dict[str, Twin] = {}
        self.handler: dict[str, Callable[..., Any]] = {}
        self.exposers: dict[str, list[Twin]] = {}
        self.subscribers: dict[str, list[Twin]] = {}
        self.last_invoked: dict[str, int] = {}
        self.shared: Any = None
        self.serve_task: Optional[asyncio.Task[None]] = None
        self.server_instance = None  # To store the server object
        self._shutdown_event = asyncio.Event()  # For controlled shutdown
        self.config = rs.Config(name)
        self.policy = Policy(policy)
        self.owners = rs.load_tenants(self)
        self.start_ts = rp.timestamp()
        self.msg_cache: list[rp.PubSubMsg] = []
        self.msg_topic_cache: dict[str, List[rp.PubSubMsg]] = {}
        self.tables: dict[str, rdb.Table] = {}
        self._builtins()
        if data_at_rest:
            self.db = rdb.init_db(self, schema)
            ival: float = float(os.getenv("REMBUS_ARCHIVER_INTERVAL", "1.0"))
            self.save_task = asyncio.create_task(self._periodic_saver(ival))
        else:
            self.db = None
            self.save_task = None

    def __str__(self):
        return f"{self.id}"

    def __repr__(self):
        twins = {t for t in self.id_twin if t != "repl"}
        return f"{self.id}: {twins}"

    @property
    def twins(self):
        """Return the twins connected to."""
        return {k: v for (k, v) in self.id_twin.items() if k != "repl"}

    def isconnected(self, rid: str) -> bool:
        """Check if a component with the given rid is connected."""
        for tk, twin in self.id_twin.items():
            if tk.startswith(rid + "@"):
                if twin.isopen():
                    return True
        return False

    def uptime(self) -> str:
        """Return the uptime of the router."""
        return f"up for {int(time.time() - self.start_ts)} seconds"

    def _builtins(self):
        self.handler["rid"] = lambda *_, **__: self.id
        self.handler["version"] = lambda *_, **__: __version__
        self.handler["uptime"] = lambda *_, **__: self.uptime()
        self.handler["python_service_install"] = partial(
            builtins.add_callback, self, "services"
        )
        self.handler["python_service_uninstall"] = partial(
            builtins.remove_callback, self, "services"
        )
        self.handler["python_subscriber_install"] = partial(
            builtins.add_callback, self, "subscribers"
        )
        self.handler["python_subscriber_uninstall"] = partial(
            builtins.remove_callback, self, "subscribers"
        )

    async def init_twin(self, uid: RbURL, enc: int, isserver: bool):
        """
        Create and start a Twin for the component that connects to a server.
        """
        cmp = Twin(uid, bottom_router(self), not isserver, enc)
        await cmp.start()
        self.id_twin[uid.twkey] = cmp

        try:
            if cmp.isrepl():
                await builtins.load_callbacks(cmp)
            else:
                if self.config.start_anyway:
                    await cmp.inbox.put("reconnect")
                else:
                    await cmp.connect()
        except Exception:
            await cmp.close()
            raise
        return cmp

    async def _shutdown(self):
        """Router cleanup logic when shutting down."""
        if self.server_instance:
            self.server_instance.close()
            self._shutdown_event.set()
            await self.server_instance.wait_closed()

        if self.serve_task is not None:
            self.save_task.cancel()

        if self.db is not None:
            self.db.execute("DETACH DATABASE __ducklake_metadata_rl")
            self.db.close()

    async def _broadcast(self, msg):
        twin = msg.twin
        data = rp.tag2df(msg.data)
        try:
            if msg.topic in self.handler:
                await self.evaluate(msg.twin, msg.topic, data)

            subs = self.subscribers.get(msg.topic, [])
            for t in subs:
                if t != twin:
                    # Do not send back to publisher.
                    await t.send(msg)
                    t.mark = msg.recvts

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("[%s] error in method invocation: %s", self, e)
            traceback.print_exc()

    async def _pubsub_msg(self, msg: rp.PubSubMsg):
        twin = msg.twin
        ts = rp.timestamp()
        msg.recvts = ts
        qos = msg.flags & rp.QOS2
        if qos > rp.QOS0 and msg.id:
            if twin.socket:
                await twin.send(rp.AckMsg(id=msg.id))

            if qos == rp.QOS2:
                if msg.id in twin.ackdf:
                    # Already received, skip the message.
                    return
                else:
                    # Save the message id to guarantee exactly one delivery.
                    # twin.ackdf[msg.id] = int(time.time())
                    twin.ackdf[msg.id] = rp.timestamp()

        if self.db is not None:
            # save the message into msg_cache
            self.append_message(msg)

        await self._broadcast(msg)

    def append_message(self, msg: rp.PubSubMsg):
        """Append a message to the message cache."""
        rdb.msg_table(self, msg)
        self.msg_cache.append(msg)
        if msg.table in self.tables:
            if msg.table not in self.msg_topic_cache:
                self.msg_topic_cache[msg.table] = []
            self.msg_topic_cache[msg.table].append(msg)

    async def send_message(self, msg):
        """Send message to remote node using a twin from the pool of twins"""
        for t in self.twins.values():
            if t.isopen():
                try:
                    futreq = t.send_task(msg)
                    data = await t.wait_response(futreq)
                except Exception as e:  # pylint:disable=broad-exception-caught
                    data = f"{e}"
                fut = await msg.twin.future_request(msg.id)
                if fut:
                    fut.future.set_result(data)
                break

    def _select_twin(self, topic):
        twins = self.exposers[topic]
        if self.policy == Policy.FIRST_UP:
            return next((t for t in twins if t.isopen()), None)
        elif self.policy == Policy.ROUND_ROBIN:
            return round_robin(self, domain(topic), topic, twins)
        elif self.policy == Policy.LESS_BUSY:
            return less_busy(domain(topic), twins)

    async def _rpcreq_msg(self, msg: rp.RpcReqMsg):
        """Handle an RPC request."""
        data = rp.tag2df(msg.data)
        logger.debug("[%s] rpc: %s", self, msg)
        if msg.twin.isrepl():
            await self.send_message(msg)
        elif msg.topic in self.handler:
            status = rp.STS_OK
            try:
                output = await self.evaluate(msg.twin, msg.topic, data)
            except Exception as e:  # pylint: disable=broad-exception-caught
                status = rp.STS_METHOD_EXCEPTION
                output = f"{e}"
                logger.debug("exception: %s", e)
            outmsg = rp.ResMsg(id=msg.id, status=status, data=rp.df2tag(output))
            await msg.twin.send(outmsg)
        elif msg.topic in self.exposers:
            target_twin = self._select_twin(msg.topic)
            logger.debug("[%s] target twin: %s", self, target_twin)
            if target_twin is None:
                outmsg = rp.ResMsg(
                    id=msg.id, status=rp.STS_METHOD_UNAVAILABLE, data=msg.topic
                )
                await msg.twin.send(outmsg)
            else:
                logger.debug("[%s] sending to [%s]", self, target_twin)
                # dispatch to the target twin
                await target_twin.inbox.put(msg)
        else:
            outmsg = rp.ResMsg(
                id=msg.id, status=rp.STS_METHOD_NOT_FOUND, data=msg.topic
            )
            await msg.twin.send(outmsg)
        return

    async def _handle_identity(self, msg: rp.IdentityMsg) -> None:
        twin_id = msg.cid
        if self.isconnected(twin_id):
            logger.warning(
                "[%s] node with id [%s] is already connected", self, twin_id
            )
            await msg.twin.close()
        else:
            logger.debug("[%s] identity: %s", self, msg.cid)
            await self._auth_identity(msg)

    async def _handle_attestation(self, msg: rp.AttestationMsg) -> None:
        sts = await self._verify_signature(msg)
        await msg.twin.response(sts, msg)

    async def _handle_admin(self, msg: rp.AdminMsg) -> None:
        logger.debug("[%s] admin: %s", self, msg)
        if msg.twin.isrepl():
            await self.send_message(msg)
        else:
            await admin_command(msg)

    async def _periodic_saver(self, interval: float = 1.0):
        """Push periodic save messages into the inbox."""
        while True:
            await asyncio.sleep(interval)
            await self.inbox.put("save_messages")

    async def _task_impl(self) -> None:
        """Switch messages between twins."""
        logger.debug("[%s] router started", self)
        while True:
            msg = await self.inbox.get()
            match msg:
                case "shutdown":
                    logger.debug("[%s] router shutting down", self)
                    break
                case "save_messages":
                    # save messages to the database periodically
                    rdb.save_data_at_rest(self)
                case rp.SendDataAtRest():
                    await rdb.send_data_at_rest(msg)
                case rp.PubSubMsg():
                    await self._pubsub_msg(msg)
                case rp.RpcReqMsg():
                    await self._rpcreq_msg(msg)
                case rp.IdentityMsg():
                    await self._handle_identity(msg)
                case rp.AttestationMsg():
                    await self._handle_attestation(msg)
                case rp.AdminMsg():
                    await self._handle_admin(msg)
                case rp.RegisterMsg():
                    await self._register_node(msg)
                case rp.UnregisterMsg():
                    await self._unregister_node(msg)

    async def evaluate(self, twin, topic: str, data: Any) -> Any:
        """Invoke the handler associate with the message topic."""
        if self.shared is not None:
            output = await get_response(
                self.handler[topic](
                    *getargs(data),
                    ctx=self.shared,
                    node=twin,
                )
            )
        else:
            output = await get_response(self.handler[topic](*getargs(data)))

        return output

    async def _client_receiver(self, ws):
        """Receive messages from the client component."""
        url = RbURL()
        twin = Twin(url, bottom_router(self), False)
        await twin.start()
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
            ping_interval=self.config.ws_ping_interval,
        ) as server:
            self.server_instance = server
            await self._shutdown_event.wait()

    def _needs_auth(self, cid: str):
        """Check if the component needs authentication."""
        try:
            rs.key_file(self.id, cid)
            return True
        except FileNotFoundError:
            return False

    async def _update_twin(self, twin, identity):
        logger.debug("[%s] setting name: [%s]", twin, identity)
        self.id_twin.pop(twin.twkey, twin)
        twin.rid = identity
        self.id_twin[twin.twkey] = twin
        if twin.db is not None:
            load_twin(twin)
            if twin.router.upstream:
                await twin.router.upstream.setup_twin(twin)

    async def _verify_signature(self, msg: rp.AttestationMsg):
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
                pubkey.verify(
                    signature, plain, padding.PKCS1v15(), hashes.SHA256()
                )
            elif isinstance(pubkey, ec.EllipticCurvePublicKey):
                pubkey.verify(signature, plain, ec.ECDSA(hashes.SHA256()))

            await self._update_twin(twin, msg.cid)
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
            data=bytes_to_b64(challenge_val, twin.enc),
        )

    async def _auth_identity(self, msg: rp.IdentityMsg):
        """Authenticate the identity of the component."""
        twin = msg.twin
        identity = msg.cid

        if self._needs_auth(identity):
            # component is provisioned, send the challenge
            response = self._challenge(msg)
        else:
            await self._update_twin(twin, identity)
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


def response_data(msg: rp.ResMsg):
    """Return the response data or raise an exception on error."""
    sts = msg.status
    if sts == rp.STS_OK:
        return rp.tag2df(msg.data)
    elif sts == rp.STS_CHALLENGE:
        return msg.data
    else:
        raise rp.RembusError(sts, msg.data)


class Twin(Supervised):
    """
    A Twin represents a Rembus component, either as a client or server.
    It handles the connection, message sending and receiving, and provides
    methods for RPC, pub/sub, and other commands interactions.
    """

    def __init__(
        self,
        uid: RbURL,
        router: Supervised,
        isclient: bool = True,
        enc: int = rp.CBOR,
    ):
        super().__init__()
        self.isclient = isclient
        self.enc = enc
        self._router = router
        # websockets.ClientConnection | MQTTClient | None
        self.socket: Any = None
        self.receiver = None
        self.uid = uid
        self.handler: dict[str, Callable[..., Any]] = {}
        self.outreq: dict[int, FutureResponse] = {}
        self.reconnect_task: Optional[asyncio.Task[None]] = None
        self.ackdf: dict[int, int] = {}  # msgid => ts
        self.handler["phase"] = lambda: "CLOSED"
        self.isreactive: bool = False
        self.msg_from: dict[str, float] = {}
        self.mark: int = 0
        # self.start()

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return self.uid.id

    def __eq__(self, other):
        return isinstance(other, Twin) and self.rid == other.rid

    def __lt__(self, other):
        return len(self.outreq) < len(other.outreq)

    @property
    def db_attach(self):
        """Return the DuckDB ATTACH directive."""
        return self.router.config.db_attach

    @property
    def db(self):
        """Return the database associated with this twin."""
        return self.router.db

    @property
    def rid(self):
        """Return the unique id of the rembus component."""
        return self.uid.id

    @rid.setter
    def rid(self, rid: str):
        self.uid.hasname = True
        self.uid.id = rid

    @property
    def twkey(self):
        """Return the twin key"""
        return f"{self.uid.twkey}"

    @property
    def router(self):
        """Return the top router associated with this twin."""
        return top_router(self._router)

    @router.setter
    def router(self, plugin: Supervised):
        self._router = plugin

    @property
    def domain(self) -> str:
        """Return the domain of the twin."""
        return domain(self.rid)

    @property
    def broker_dir(self) -> str:
        """Return the directory of this broker."""
        return rs.broker_dir(self.router.id)

    def isrepl(self) -> bool:
        """Check if twin is a REPL"""
        return self.uid.protocol == "repl"

    @property
    def isws(self) -> bool:
        """Check if twin is a WebSocket connection"""
        return self.uid.protocol in ["ws", "wss"]

    @property
    def ismqtt(self) -> bool:
        """Check if twin is a MQTT connection"""
        return self.uid.protocol in ["mqtt", "mqtts"]

    def isopen(self) -> bool:
        """Check if the connection is open."""
        if self.isrepl():
            return any([t.isopen() for t in self.router.twins.values()])
        elif self.socket is None:
            return False
        elif self.isws:
            return self.socket.state == websockets.State.OPEN
        else:
            return self.socket.is_connected

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

    def register_shutdown(self):
        """Register shutdown handler."""
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(
            signal.SIGINT, lambda: asyncio.create_task(self.close())
        )
        loop.add_signal_handler(
            signal.SIGTERM, lambda: asyncio.create_task(self.close())
        )

    async def _shutdown(self):
        """Twin cleanup logic when shutting down."""
        logger.debug("[%s] twin shutdown", self)

        if self.db is not None:
            save_twin(self)

        if self.uid.isrepl():
            await self._shutdown_twins()

        if self.isclient or self.uid.isrepl():
            await self._shutdown_router()

        await self._close_socket()
        await self._cancel_task("receiver")
        await self._cancel_task("reconnect_task")

    async def _shutdown_router(self):
        if self.router:
            await self.router.shutdown()

    async def _close_socket(self):
        if self.socket:
            if self.isws:
                await self.socket.close()
            else:
                # MQTT
                await self.socket.disconnect()

            self.socket = None

    async def _cancel_task(self, name: str):
        task = getattr(self, name, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            setattr(self, name, None)

    async def _shutdown_twins(self):
        # Close the twins (or the components of the pool)
        for t in list(self.router.id_twin.values()):
            t.handler["phase"] = lambda: "CLOSED"
            if t.socket is not None:
                await t.shutdown()

    async def _task_impl(self):
        logger.debug("[%s] task started", self)
        while True:
            msg: Any = await self.inbox.get()
            logger.debug("[%s] twin_task: %s", self, msg)
            if msg == "reconnect":
                if not self.reconnect_task:
                    self.reconnect_task = asyncio.create_task(self._reconnect())
            elif msg == "shutdown":
                break
            elif isinstance(msg, rp.RpcReqMsg):
                await self._rpc_to_target(msg)

    async def _rpc_to_target(self, msg):
        await self.send(msg)
        futreq = FutureResponse(None, msg.data)
        self.outreq[msg.id] = futreq
        response = await self.wait_response(futreq)
        await msg.twin.send(response)

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
            websockets.ConnectionClosedError,
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

            self.receiver = None

    async def future_request(self, msgid: int):
        """Return the future associated with the message id `msgid`."""
        fut = self.outreq.pop(msgid, None)
        if fut is None:
            logger.warning(
                "[%s] recv unknown msg id %s",
                self,
                rp.tohex(rp.to_bytes(msgid)),
            )
        elif fut.future.done():
            return None

        return fut

    async def _response_msg(self, msg: rp.ResMsg):
        fut = await self.future_request(msg.id)
        if fut:
            fut.future.set_result(msg)

    async def _ack_msg(self, msg: rp.AckMsg):
        logger.debug("[%s] pubsub ack: %s", self, msg.id)
        fut = await self.future_request(msg.id)
        if fut:
            if fut.data:
                await self.send(rp.Ack2Msg(id=msg.id))

            fut.future.set_result(True)

    async def _ack2_msg(self, msg: rp.Ack2Msg):
        mid = msg.id
        if mid in self.ackdf:
            logger.debug("[%s] ack2: deleting pubsub ack %s", self, mid)
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
            self._router.inbox.put_nowait(msg)

    async def connect(self):
        """Connect to the broker."""
        if self.uid.protocol in ["ws", "wss"]:
            await self.wsconnect()
        elif self.uid.protocol in ["mqtt", "mqtts"]:
            await self.mqttconnect()

    async def on_message(self, client, topic, payload, qos, properties):
        """Route incoming mqtt messages to the router inbox."""
        logger.debug("[MQTT] (QOS=%s) %s: %s", qos, topic, payload)

        data = json.loads(payload)

        msg = rp.PubSubMsg(
            topic=topic,
            data=data if isinstance(data, list) else [data],
            flags=qos,
        )
        logger.debug("[%s] mqtt msg: %s", self, msg)
        await self._router.inbox.put(msg)

    async def mqttconnect(self):
        """Connect to an mqtt endpoint."""
        client = MQTTClient(self.uid.id)
        ssl_context = False
        if self.uid.protocol == "mqtts":
            ssl_context = True
            # pylint: disable=protected-access
            client._ssl_context = get_ssl_context()

        await client.connect(
            self.uid.hostname, self.uid.port or 1883, ssl=ssl_context
        )

        client.on_message = self.on_message
        client.subscribe("#", qos=1, no_local=True)
        self.socket = client

    async def wsconnect(self):
        """Connect to a ws endpoint."""
        broker_url = self.uid.connection_url()
        ssl_context = None
        if self.uid.protocol == "wss":
            ssl_context = get_ssl_context()

        self.socket = await websockets.connect(
            broker_url,
            ping_interval=self.router.config.ws_ping_interval,
            max_size=rp.WS_FRAME_MAXSIZE,
            ssl=ssl_context,
        )
        self.handler["phase"] = lambda: "CONNECTING"
        self.receiver = asyncio.create_task(self.twin_receiver())

        if self.uid.hasname:
            try:
                await self._login()
            except Exception as e:
                await self.close()
                raise rp.RembusError(rp.STS_ERROR, "_login failed") from e

        self.handler["phase"] = lambda: "CONNECTED"
        return self

    async def send(self, msg: rp.RembusMsg):
        """Send a rembus message"""
        if self.isws:
            pkt = msg.to_payload(self.enc)
            await self._send(pkt)

        else:
            # MQTT publish
            if isinstance(msg, rp.PubSubMsg):
                data = msg.data if len(msg.data) > 1 else msg.data[0]
                self.socket.publish(msg.topic, json.dumps(data))

    async def _send(self, payload: bytes | str) -> Any:
        if self.socket is not None:
            await self.socket.send(payload)

    def _qos_send(self, msgid: int, msg: rp.PubSubMsg) -> Any:
        """Send a pubsub message and wait for the ack."""
        task = asyncio.create_task(self.send(msg))
        futreq = FutureResponse(task, msg.flags & rp.QOS2 == rp.QOS2)
        self.outreq[msgid] = futreq
        return futreq

    async def _send_message(self, builder: Callable, data: Any = None) -> Any:
        """Send a message and wait for a response."""
        reqid = rp.msgid()
        req = builder(reqid)
        req.twin = self
        task = None
        if self.isrepl() and self.isopen():
            await self._router.inbox.put(req)
        elif self.socket is None:
            raise rp.RembusConnectionClosed()
        else:
            task = asyncio.create_task(self.send(req))

        futreq = FutureResponse(task, data)
        self.outreq[reqid] = futreq
        return futreq

    def send_task(self, msg: rp.RpcReqMsg) -> Any:
        """Send a message and wait for a response."""
        task = asyncio.create_task(self.send(msg))
        futreq = FutureResponse(task, msg.data)
        self.outreq[msg.id] = futreq
        return futreq

    async def wait_response(self, futreq):
        try:
            async with async_timeout.timeout(
                self.router.config.request_timeout
            ):
                return await futreq.future
        except TimeoutError as e:
            raise rp.RembusTimeout() from e

    async def _login(self):
        """Connect in free mode or authenticate the provisioned component."""
        futreq = await self._send_message(
            lambda id: rp.IdentityMsg(id=id, cid=self.uid.id)
        )
        response = await self.wait_response(futreq)
        challenge = response_data(response)
        if isinstance(challenge, bytes | str):
            if isinstance(challenge, str):
                challenge = base64.b64decode(challenge)

            plain = [bytes(challenge), self.uid.id]
            message = cbor2.dumps(plain)
            privatekey = rp.load_private_key(self.uid.id)
            if isinstance(privatekey, rsa.RSAPrivateKey):
                signature: bytes = privatekey.sign(
                    message, padding.PKCS1v15(), hashes.SHA256()
                )
            elif isinstance(privatekey, ec.EllipticCurvePrivateKey):
                signature: bytes = privatekey.sign(
                    message, ec.ECDSA(hashes.SHA256())
                )

            futreq = await self._send_message(
                lambda id: rp.AttestationMsg(
                    id=id,
                    cid=self.uid.id,
                    signature=bytes_to_b64(signature, self.enc),
                )
            )
            response = await self.wait_response(futreq)
            response_data(response)
        else:
            logger.debug("[%s]: free mode access", self)

    async def publish(self, topic: str, *data: Any, **kwargs):
        """Publish a message to the specified topic."""

        slot = kwargs.get("slot", None)
        qos = kwargs.get("qos", rp.QOS0) & rp.QOS2
        if qos == rp.QOS0:
            logger.info("topic=%s data=%s", topic, data)
            msg = rp.PubSubMsg(topic=topic, data=data, slot=slot)
            msg.twin = self
            if self.isrepl() and self.isopen():
                await self._router.inbox.put(msg)
            elif self.socket is None:
                raise rp.RembusConnectionClosed()
            else:
                await self.send(msg)
        else:
            await self._qos_publish(topic, data, qos, slot)

        return None

    async def put(self, topic: str, *args: Any, **kwargs):
        """Publish a message to the topic prefixed with component name."""
        await self.publish(self.rid + "/" + topic, *args, **kwargs)

    async def _qos_publish(
        self,
        topic: str,
        data: tuple,
        qos: rp.UInt8,  # type: ignore[valid-type]
        slot: int | None,
    ):
        done = False
        max_retries = self.router.config.send_retries
        retries = 0
        if slot is None:
            reqid = rp.msgid()
        else:
            reqid = rp.msgid_slot(slot)

        while True:
            retries += 1
            try:
                futreq = self._qos_send(
                    reqid,
                    rp.PubSubMsg(
                        id=reqid, topic=topic, data=data, flags=qos, slot=slot
                    ),
                )
                async with async_timeout.timeout(
                    self.router.config.request_timeout
                ):
                    done = await futreq.future
                if done:
                    break
            except TimeoutError as e:
                if retries > max_retries:
                    raise rp.RembusTimeout() from e

    async def broker_setting(self, command: str, args: dict[str, Any]):
        """Send a broker configuration command."""
        data = {rp.COMMAND: command} | args
        futreq = await self._send_message(
            lambda id: rp.AdminMsg(id=id, topic=rp.BROKER_CONFIG, data=data)
        )
        response = await self.wait_response(futreq)
        return response_data(response)

    async def setting(
        self, topic: str, command: str, args: dict[str, Any] | None = None
    ):
        """Send an admin command to the broker."""
        if self.socket:
            if args:
                data = {rp.COMMAND: command} | args
            else:
                data = {rp.COMMAND: command}

            futreq = await self._send_message(
                lambda id: rp.AdminMsg(id=id, topic=topic, data=data)
            )
            response = await self.wait_response(futreq)
            return response.data

    async def rpc(self, topic: str, *args: Any):
        """Send a RPC request."""
        data = rp.df2tag(args)
        futreq = await self._send_message(
            lambda id: rp.RpcReqMsg(id=id, topic=topic, data=data)
        )
        response = await self.wait_response(futreq)
        return response_data(response)

    async def direct(self, target: str, topic: str, *args: Any):
        """Send a RPC request to a specific target."""
        data = rp.df2tag(args)
        futreq = await self._send_message(
            lambda id: rp.RpcReqMsg(
                id=id, topic=topic, target=target, data=data
            )
        )
        response = await self.wait_response(futreq)
        return response_data(response)

    async def register(self, rid: str, pin: str, scheme: int = rp.SIG_RSA):
        """Provisions the component with rid identifier."""
        if scheme == rp.SIG_RSA:
            privkey = rp.rsa_private_key()
        else:
            privkey = rp.ecdsa_private_key()

        pubkey = rp.pem_public_key(privkey)
        if self.enc == rp.JSON:
            pubkey = base64.b64encode(pubkey).decode("utf-8")

        futreq = await self._send_message(
            lambda id: rp.RegisterMsg(
                id=id, cid=rid, pin=pin, pubkey=pubkey, type=scheme
            )
        )
        response = await self.wait_response(futreq)
        response_data(response)

        logger.debug("cid %s registered", rid)
        rp.save_private_key(rid, privkey)
        return None

    async def unregister(self):
        """Unprovisions the component."""
        futreq = await self._send_message(lambda id: rp.UnregisterMsg(id=id))
        response = await self.wait_response(futreq)
        with suppress(FileNotFoundError):
            os.remove(os.path.join(rs.rembus_dir(), self.uid.id, ".secret"))
        return response_data(response)

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
        msgfrom: float = rp.Now,
        topic: Optional[str] = None,
    ):
        """
        Subscribe the function to the corresponding topic.
        """
        if topic is None:
            topic = fn.__name__

        if self.isrepl():
            await self._router.subscribe_handler(self, topic)
        else:
            await self.setting(topic, rp.ADD_INTEREST, {"msg_from": msgfrom})

        self.router.handler[topic] = fn
        return self

    async def unsubscribe(self, fn: Callable[..., Any] | str):
        """
        Unsubscribe the function from the corresponding topic.
        """
        if isinstance(fn, str):
            topic = fn
        else:
            topic = fn.__name__

        if self.isrepl():
            await self._router.unsubscribe_handler(self, topic)
        else:
            await self.setting(topic, rp.REMOVE_INTEREST)

        self.router.handler.pop(topic, None)
        return self

    async def expose(self, fn: Callable[..., Any], topic: Optional[str] = None):
        """
        Expose the function as a remote procedure call(RPC) handler.
        """
        if topic is None:
            topic = fn.__name__

        self.router.handler[topic] = fn
        await self.setting(topic, rp.ADD_IMPL)

    async def unexpose(
        self, fn: Callable[..., Any] | str, topic: Optional[str] = None
    ):
        """
        Unexpose the function as a remote procedure call(RPC) handler.
        """
        if isinstance(fn, str):
            topic = fn
        else:
            topic = fn.__name__

        self.router.handler.pop(topic, None)
        await self.setting(topic, rp.REMOVE_IMPL)

    async def close(self):
        """Close the connection and clean up resources."""
        self.handler["phase"] = lambda: "CLOSED"
        await self.shutdown()
        await asyncio.sleep(0)  # let loop drain tasks

    async def wait(self, timeout: float | None = None):
        """
        Start the twin event loop that wait for rembus messages.
        """
        if not self.isrepl():
            await self.reactive()
        if self._supervisor_task is not None:
            try:
                await asyncio.wait([self._supervisor_task], timeout=timeout)
            finally:
                await self.shutdown()


def top_router(router: Supervised) -> Router:
    """Return the topmost router (the core router)."""
    r = router
    while r.downstream is not None:
        r = r.downstream
    return cast(Router, r)


def bottom_router(router: Supervised) -> Supervised:
    """
    Return the router attached to the twins (the lowest router in the chain).
    """
    r = router
    while r.upstream is not None:
        r = r.upstream
    return r


def alltwins(router):
    """Get all the connected twins."""
    r = top_router(router)
    return [tw for tw in r.id_twin.values() if tw.rid != "__repl__"]


async def add_plugin(twin: Twin, plugin: Supervised):
    """Add a plugin router in front of the current twin router."""
    router = twin.router
    router.upstream = plugin
    plugin.downstream = router
    twin.router = plugin
    await plugin.start()
    logger.debug("[%s] added plugin %s", twin, plugin)
    for tw in alltwins(router):
        tw.router = plugin


def sync_table(
    db, table_name: str, current_df: pl.DataFrame, new_df: pl.DataFrame
):
    """
    Synchronize a db table by removing rows not in `new_df`
    and adding rows not in `current_df`.
    """
    fields = current_df.columns

    conds = ["df.name = t.name"]
    for field in fields:
        conds.append(f"df.{field} = t.{field}")
    cond_str = " AND ".join(conds)

    # Find rows in current_df that are not in new_df (rows to delete)
    df = current_df.join(new_df, on=fields, how="anti")

    if not df.is_empty():
        # Delete rows that exist in current but not in new
        db.sql(f"""
            DELETE FROM {table_name} t WHERE EXISTS (
                SELECT 1 FROM df WHERE {cond_str}
            )
        """)

    # Find rows in new_df that are not in current_df (rows to insert)
    df = new_df.join(current_df, on=fields, how="anti")

    if not df.is_empty():
        db.sql(f"INSERT INTO {table_name} SELECT * FROM df")


def sync_twin(
    db, router_name: str, twin_name: str, table_name: str, new_df: pl.DataFrame
):
    """Synchronize a twin's data in a specific table."""
    current_df = db.sql(
        f"""
        SELECT * FROM {table_name} 
        WHERE name = ? AND twin = ?
    """,
        params=[router_name, twin_name],
    ).pl()

    sync_table(db, table_name, current_df, new_df)


# def sync_cfg(db, router_name: str, table_name: str, new_df: pl.DataFrame):
#    """Synchronize configuration data for a router."""
#    current_df = db.sql(
#        f"""
#        SELECT * FROM {table_name}
#        WHERE name = ?
#    """,
#        params=[router_name],
#    ).pl()
#
#    sync_table(db, table_name, current_df, new_df)


def save_twin(twin):
    """
    Save twin data (subscribers, exposers, and marks) to the database.
    """
    router = twin.router
    tid = twin.rid
    name = router.id
    db = router.db

    # Save subscriber data
    if twin.msg_from:
        current_df = pl.DataFrame(
            {
                "name": [name] * len(twin.msg_from),
                "twin": [tid] * len(twin.msg_from),
                "topic": list(twin.msg_from.keys()),
                "msg_from": list(twin.msg_from.values()),
            }
        )
        sync_twin(db, name, tid, "subscriber", current_df)

    # Save exposer data
    exposed_topics = exposed_topics_for_twin(router, twin)
    if exposed_topics:
        current_df = pl.DataFrame(
            {
                "name": [name] * len(exposed_topics),
                "twin": [tid] * len(exposed_topics),
                "topic": exposed_topics,
            }
        )
        sync_twin(db, name, tid, "exposer", current_df)

    # Save mark data
    current_df = pl.DataFrame(
        {"name": [name], "twin": [tid], "mark": [twin.mark]}
    )
    sync_twin(db, name, tid, "mark", current_df)


def exposed_topics_for_twin(router, twin) -> List[str]:
    """
    Get list of topics exposed by this twin.
    """
    exposed = []
    for topic, twins in router.exposers.items():
        if twin in twins:
            exposed.append(topic)
    return exposed


def load_twin(twin):
    """
    Load twin data (subscribers, exposers, and marks) from the database.
    """
    router = twin.router
    name = router.id
    tid = twin.rid
    db = router.db

    df = db.sql(
        "SELECT topic, msg_from FROM subscriber WHERE name = ? AND twin = ?",
        params=[name, tid],
    ).pl()

    if not df.is_empty():
        twin.msg_from = dict(
            zip(df["topic"].to_list(), df["msg_from"].to_list())
        )

        # Update router's subscribers
        for topic in df["topic"].to_list():
            if topic not in router.subscribers:
                router.subscribers[topic] = []
            if twin not in router.subscribers[topic]:
                router.subscribers[topic].append(twin)

    df = db.sql(
        "SELECT topic FROM exposer WHERE name = ? AND twin = ?",
        params=[name, tid],
    ).pl()

    if not df.is_empty():
        # Update router's exposers
        for topic in df["topic"].to_list():
            if topic not in router.exposers:
                router.exposers[topic] = []
            if twin not in router.exposers[topic]:
                router.exposers[topic].append(twin)

    # Load mark data
    result = db.sql(
        "SELECT mark FROM mark WHERE name = ? AND twin = ?", params=[name, tid]
    ).fetchone()

    if result:
        twin.mark = result[0]
        logger.debug("[%s] loaded mark %s", twin, twin.mark)
    else:
        logger.debug("[%s] no mark found in database", twin)
