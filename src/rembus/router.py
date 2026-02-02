"""Router dispatch messages between twins."""

from __future__ import annotations
import asyncio
import base64
from enum import Enum
from functools import partial
import logging
import os
import time
import traceback
from typing import Callable, Any, Optional, List, cast
import ssl
from websockets.asyncio.server import serve
import cbor2
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa, ec
import rembus.protocol as rp
import rembus.settings as rs
import rembus.db as rdb
import rembus.builtins as builtins
from . import __version__
from .admin import admin_command
from .core import Supervised, RbURL, domain, bytes_to_b64


logger = logging.getLogger(__name__)


class Policy(Enum):
    """Load balancing policies for selecting twins."""

    FIRST_UP = "first_up"
    ROUND_ROBIN = "round_robin"
    LESS_BUSY = "less_busy"


async def get_response(obj: Any) -> Any:
    """Return the response of the object."""
    if asyncio.iscoroutine(obj):
        return await obj
    else:
        return obj


def getargs(data):
    """
    Return arguments list from the data.
    """
    if isinstance(data, list):
        return data
    else:
        return [data]


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
        load_admins(router)
        load_tenants(router)

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
        self.admins: dict = {}
        self.id_twin: dict = {}
        self.handler: dict[str, Callable[..., Any]] = {}
        self.exposers: dict = {}
        self.subscribers: dict = {}
        self.private_topics: dict = {}
        self.last_invoked: dict[str, int] = {}
        self.shared: Any = None
        self.serve_task: Optional[asyncio.Task[None]] = None
        self.server_instance = None  # To store the server object
        self._shutdown_event = asyncio.Event()  # For controlled shutdown
        self.config = rs.Config(name)
        self.policy = Policy(policy)
        self.tenants: dict = {}
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

    async def _shutdown(self):
        """Router cleanup logic when shutting down."""
        if self.server_instance:
            self.server_instance.close()
            self._shutdown_event.set()
            await self.server_instance.wait_closed()

        if self.save_task is not None:
            self.save_task.cancel()

        if self.db is not None:
            self.db.execute("DETACH DATABASE __ducklake_metadata_rl")
            self.db.close()

    async def _broadcast(self, msg):
        twin = msg.twin
        data = rp.tag2df(msg.data)
        topic = msg.topic
        try:
            if topic in self.handler and self.isauthorized(topic, twin):
                await self.evaluate(twin, topic, data)

            subs = self.subscribers.get(topic, [])
            for t in subs:
                if t != twin and self.isauthorized(topic, t):
                    # Do not send back to publisher.
                    await t.send(msg)
                    t.mark = msg.recvts

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("[%s] error in method invocation: %s", self, e)
            traceback.print_exc()

    async def _pubsub_msg(self, msg: rp.PubSubMsg):
        twin = msg.twin

        # Check if publisher is authorized to
        if self.isauthorized(msg.topic, twin):
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
                        twin.ackdf[msg.id] = rp.timestamp()

            if self.db is not None:
                # Save the message into msg_cache
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

    def isauthorized(self, topic: str, twin):
        """Check if the component is authorized.
        If the topic is public the component is always authorized.
        """
        if topic in self.private_topics:
            if twin.rid in self.private_topics[topic]:
                return True
            else:
                return False

        return True

    async def _rpcreq_msg(self, msg: rp.RpcReqMsg):
        """Handle an RPC request."""
        data = rp.tag2df(msg.data)
        logger.debug("[%s] rpc: %s", self, msg)
        topic = msg.topic
        if msg.twin.isrepl():
            await self.send_message(msg)
        elif topic in self.handler and self.isauthorized(topic, msg.twin):
            status = rp.STS_OK
            try:
                output = await self.evaluate(msg.twin, topic, data)
            except Exception as e:  # pylint: disable=broad-exception-caught
                status = rp.STS_METHOD_EXCEPTION
                output = f"{e}"
                logger.debug("exception: %s", e)
            outmsg = rp.ResMsg(id=msg.id, status=status, data=rp.df2tag(output))
            await msg.twin.send(outmsg)
        elif topic in self.exposers and self.isauthorized(topic, msg.twin):
            target_twin = self._select_twin(topic)
            logger.debug("[%s] target twin: %s", self, target_twin)
            if target_twin is None:
                outmsg = rp.ResMsg(
                    id=msg.id, status=rp.STS_METHOD_UNAVAILABLE, data=topic
                )
                await msg.twin.send(outmsg)
            else:
                logger.debug("[%s] sending to [%s]", self, target_twin)
                # dispatch to the target twin
                await target_twin.inbox.put(msg)
        else:
            outmsg = rp.ResMsg(
                id=msg.id, status=rp.STS_METHOD_NOT_FOUND, data=topic
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
        """Receive messages from the client component.

        NOTE: Local import to avoid circular dependency with rembus.twin
        """
        # pylint:disable=import-outside-toplevel
        from rembus.twin import WsTwin

        url = RbURL()
        twin = WsTwin(url, bottom_router(self), False)
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
            # Component is provisioned, send the challenge
            response = self._challenge(msg)
        else:
            await self._update_twin(twin, identity)
            response = rp.ResMsg(id=msg.id, status=rp.STS_OK)

        await twin.send(response)

    def _get_token(self, tenant, secret: str):
        """Get the token embedded into the message id."""
        pin = self.tenants.get(tenant)
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


async def add_plugin(twin, plugin: Supervised):
    """Add a plugin router in front of the current twin router."""
    router = twin.router
    router.upstream = plugin
    plugin.downstream = router
    twin.router = plugin
    await plugin.start()
    logger.debug("[%s] added plugin %s", twin, plugin)
    for tw in alltwins(router):
        tw.router = plugin


def load_admins(router):
    """
    Load admin components.
    """
    db = router.db
    result = db.sql("SELECT twin FROM admin WHERE name = ?", params=[router.id])
    router.admins = result.df()["twin"].to_list()
    logger.info("[%s] admins: %s", router, router.admins)


def load_tenants(router):
    """Load the tenants."""
    db = router.db
    result = db.sql(
        "SELECT tenant, secret FROM tenant WHERE name = ?", params=[router.id]
    )
    router.tenants = {row[0]: row[1] for row in result.fetchall()}


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

        # Update router's subscribers.
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
        # Update router's exposers.
        for topic in df["topic"].to_list():
            if topic not in router.exposers:
                router.exposers[topic] = []
            if twin not in router.exposers[topic]:
                router.exposers[topic].append(twin)

    # Load messages marks.
    result = db.sql(
        "SELECT mark FROM mark WHERE name = ? AND twin = ?", params=[name, tid]
    ).fetchone()

    if result:
        twin.mark = result[0]
        logger.debug("[%s] loaded mark %s", twin, twin.mark)
    else:
        logger.debug("[%s] no mark found in database", twin)
