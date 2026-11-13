"""Router dispatch messages between twins."""

from __future__ import annotations
import asyncio
import base64
import hmac
from enum import Enum
from functools import partial
import ipaddress
import logging
import os
import time
from datetime import datetime, timezone
import traceback
from typing import Callable, Any, Optional, List, cast
import socket
import ssl
import websockets
from websockets.asyncio.server import serve
import cbor2
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa, ec
import rembus.protocol as rp
import rembus.settings as rs
import rembus.db as rdb
import rembus.builtins as builtins
import rembus.telemetry as telemetry
from . import __version__
from .admin import admin_command, add_exposer, add_subscriber, ismultipath
from .tenant import MODE_OPAQUE, MODE_PIN
from .core import (
    Supervised,
    RbURL,
    tenant_of,
    valid_cid,
    bytes_to_b64,
    Node,
    mesh_nodes,
)


logger = logging.getLogger(__name__)

PIN_NONCE_SIZE = 32
PIN_NONCE_TTL = 60.0  # seconds
MAX_PIN_NONCES = 8  # outstanding nonces per connection
OPAQUE_TTL = 60.0  # seconds, lifetime of a stage 1 exchange
OPAQUE_MAX_EXCHANGES = 8  # outstanding OPAQUE exchanges per connection
BUILTIN_TOPICS = frozenset({"rid", "version", "uptime"})
"""
Topic names registered in `Router.handler` that are built-in admin
methods (see `Router._builtins`), excluded from the mesh exports a node
advertises to its peers (see `Router.local_exports`/`topic_impls`/
`topic_interests`). Mirrors Rembus.jl's `isbuiltin`.
"""

__all__ = ["bottom_router", "top_router", "Router"]


class Policy(Enum):
    """Load balancing policies for selecting twins."""

    FIRST_UP = "first_up"
    ROUND_ROBIN = "round_robin"
    LESS_BUSY = "less_busy"


def _skip_reason(router, t, twin, topic) -> str:
    if not t.isreactive:
        return "not reactive"
    if not t.isopen():
        return "disconnected"
    if t.tenant != twin.tenant:
        return "different tenant"
    return "not authorized" if not router.isauthorized(topic, t) else ""


async def get_response(obj: Any) -> Any:
    """Return the response of the object."""
    if asyncio.iscoroutine(obj):
        return await obj
    else:
        return obj


def getargs(data):
    """
    Return arguments list from the data.

    ``data`` already represents multiple positional arguments when it is a
    ``list`` (e.g. messages decoded off the wire, see
    :meth:`~rembus.twin.MqttTwin.process_message`) or a ``tuple`` (e.g.
    in-process :class:`~rembus.protocol.PubSubMsg` payloads built from
    ``Twin.publish``'s ``*data``, see :meth:`~rembus.twin.Twin._publish`).
    In both cases the items should be unpacked as separate positional
    arguments; ``None`` means no arguments; any other value is treated as a
    single argument.
    """
    if data is None:
        return []
    if isinstance(data, (list, tuple)):
        return data
    else:
        return [data]


def first_up(tenant, implementors):
    """Select the first open twin belonging to the tenant."""
    return next(
        (t for t in implementors if t.isopen() and t.tenant == tenant), None
    )


def less_busy(tenant, implementors):
    """Select the twin with fewer outstanding requests."""
    up_and_running = [
        t for t in implementors if t.isopen() and t.tenant == tenant
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
            if candidate is None and tw.isopen() and tw.tenant == tenant:
                candidate = tw
                candidate_index = ix

        else:
            # forward search from current_index
            if tw.isopen() and tw.tenant == tenant:
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


async def init_router(
    router_name,
    policy,
    uid,
    port,
    secure,
    isserver,
    schema,
    relay: bool = True,
    authenticated: bool = False,
):
    """Start the router"""

    if policy not in ["first_up", "less_busy", "round_robin"]:
        raise ValueError(
            "wrong routing policy, must be one of first_up, "
            "less_busy, round_robin"
        )

    router = Router(
        router_name, policy, isserver, schema, relay, authenticated
    )
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
    The core Router routes messages between components, each represented
    by a :class:`~rembus.twin.Twin` instance.
    """

    def __init__(
        self,
        name: str,
        policy: str = "first_up",
        data_at_rest: bool = True,
        schema: str | None = None,
        relay: bool = True,
        authenticated: bool = False,
    ):
        super().__init__()
        # False for a "server": RPC requests from ordinary components are
        # resolved only by the server's own handlers and are not relayed
        # to other connected components (see Rembus.jl server_task).
        self.relay = relay
        self.wsport = None
        self.wsprotocol = "ws"
        self.network: List[Node] = []
        self.id = name
        self.eid = int.from_bytes(os.urandom(8), "big")
        """
        Ephemeral random unsigned 64-bit id of this router instance, used to stamp mesh-wide
        admin commands (``rmark``) so that a flood of
        ``subscribe``/``expose``/``unsubscribe``/``unexpose`` never loops
        back and gets re-applied to the same router twice (see
        :func:`rembus.admin.mark_and_broadcast`).
        """
        self.admins: dict = {}
        self.id_twin: dict = {}
        self.handler: dict[str, Callable[..., Any]] = {}
        self.exposers: dict = {}
        self.subscribers: dict = {}
        self.local_subscriber: dict[str, float] = {}
        """
        `msg_from` timestamp of every topic this node has *directly*
        subscribed to (via :meth:`Twin.subscribe`), keyed by topic. Used
        to tell apart, among the non-builtin keys of `self.handler`,
        which ones are local subscriber callbacks versus local RPC
        exposers (see `local_exports`/`topic_impls`/`topic_interests`).
        Mirrors Rembus.jl's `router.local_subscriber`.
        """
        self.private_topics: dict = {}
        self.last_invoked: dict[str, int] = {}
        self.shared: Any = None
        self.serve_task: Optional[asyncio.Task[None]] = None
        self.server_instance = None  # To store the server object
        self._shutdown_event = asyncio.Event()  # For controlled shutdown
        self.config = rs.Settings(name)
        # If True only components that have proved their identity are
        # allowed to send commands (see `command_permitted`).
        self.authenticated = (
            authenticated or self.config.connection_mode == "authenticated"
        )
        self.policy = Policy(policy)
        self.tenants: dict = {}
        # tenant -> OPAQUE password file
        self.opaque_tenants: dict[str, bytes] = {}
        # cid -> pin and cid -> OPAQUE password file (secrets of a cid)
        self.cid_pins: dict[str, str] = {}
        self.cid_opaque: dict[str, bytes] = {}
        # tenant -> explicitly set mode (otherwise derived from the secrets)
        self.tenant_modes: dict[str, str] = {}
        self._opaque_setup: Any = None
        self.start_ts = rp.timestamp()
        self.msg_cache: list[rp.PubSubMsg] = []
        self.msg_topic_cache: dict[str, List[rp.PubSubMsg]] = {}
        self.tables: dict[str, rdb.Table] = {}
        # Tracks the in-flight background `save_data_at_rest` thread (see
        # `_task_impl`'s "save_messages" case): at most one runs at a
        # time so concurrent threads never touch `self.db` together;
        # while one is running, newly-cached messages simply accumulate
        # for the next periodic save instead of blocking pub/sub
        # dispatch on this (synchronous, potentially slow under real
        # traffic) DuckLake write.
        self._data_at_rest_task: Optional[asyncio.Task[None]] = None
        # Strong references to in-flight local-RPC-handler tasks (see
        # `_rpcreq_msg`'s local-handler branch): `_task_impl` is a
        # single-consumer dispatch loop that also delivers every other
        # queued message (including pub/sub responses a handler itself
        # may be waiting on, e.g. an application-level command request), so a
        # locally-exposed RPC handler must never be `await`ed inline
        # there -- doing so can self-deadlock whenever the handler
        # blocks on something only deliverable by this same loop.
        # Tracked here (rather than only via the task's own return
        # value) so these fire-and-forget tasks aren't garbage
        # collected mid-flight.
        self._rpc_tasks: set[asyncio.Task] = set()
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
        key = RbURL(rid).twkey
        return key in self.id_twin and self.id_twin[key].isopen()

    def get_twin(self, rid: str):
        """Return the twin with name `rid`."""
        key = RbURL(rid).twkey
        return self.id_twin.get(key, None)

    def uptime(self) -> str:
        """Return the uptime of the router."""
        uptime = (rp.timestamp() - self.start_ts) / 1_000_000_000
        return f"up for {uptime} seconds"

    def _builtins(self):
        self.handler["rid"] = lambda *_, **__: self.id
        self.handler["version"] = lambda *_, **__: f"python_{__version__}"
        self.handler["uptime"] = lambda *_, **__: self.uptime()

        self.handler["python_service_list"] = partial(
            builtins.list_callback, self, "services"
        )
        self.handler["python_service_install"] = partial(
            builtins.add_callback, self, "services"
        )
        self.handler["python_service_uninstall"] = partial(
            builtins.remove_callback, self, "services"
        )
        self.handler["python_subscriber_list"] = partial(
            builtins.list_callback, self, "subscribers"
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

    def _log_delivery_failure(self, topic, twin, error):
        logger.warning(
            "[%s] failed to deliver message on topic %s to %s: %s",
            self,
            topic,
            twin,
            error,
        )

    async def _deliver_with_ack(self, twin, msg):
        """Deliver a QOS1/QOS2 message to a subscriber and update its mark."""
        try:
            await twin.deliver_with_ack(msg)
            if msg.recvts is not None:
                twin.mark = msg.recvts
        except Exception as e:  # pylint: disable=broad-exception-caught
            self._log_delivery_failure(msg.topic, twin, e)

    async def _broadcast(self, msg):
        twin = msg.twin
        data = rp.tag2df(msg.data)
        topic = msg.topic
        try:
            if topic in self.handler and self.isauthorized(topic, twin):
                await self.evaluate(twin, topic, data)

            # A server relays to other components only the messages
            # published by a broker peer (see Rembus.jl pubsub_msg).
            if not self.relay and not twin.isbroker():
                return

            await self._fanout(msg)

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("[%s] error in method invocation: %s", self, e)
            traceback.print_exc()

    async def broadcast_rpc_result(self, req: rp.RpcReqMsg):
        """Publish the arguments of a successful RPC to the topic
        subscribers, excluding the requestor."""
        msg = rp.PubSubMsg(topic=req.topic, data=req.data)
        msg.twin = req.twin
        try:
            await self._fanout(msg)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("[%s] error broadcasting rpc result: %s", self, e)

    async def _fanout(self, msg):
        twin = msg.twin
        topic = msg.topic
        subs = self.subscribers.get(topic, [])
        qos = msg.flags & rp.QOS2
        targets = []
        tasks = []
        if telemetry.ENABLED:
            telemetry.published(self, msg, subs)
        for t in subs:
            # Do not send back to publisher, only deliver to
            # subscribers that opted-in with reactive(), and only
            # to twins belonging to the same tenant as the publisher.
            # Disconnected subscribers are skipped: they recover the
            # missed messages through the data-at-rest replay.
            if (
                t != twin
                and t.isreactive
                and t.isopen()
                and t.tenant == twin.tenant
                and self.isauthorized(topic, t)
            ):
                if telemetry.ENABLED:
                    telemetry.sent(self, msg, t, True)
                if qos > rp.QOS0:
                    # QOS1/QOS2: mirror the publisher->broker hop
                    # with a full Ack/Ack2 handshake, retrying on
                    # timeout. The wait runs in a background task so
                    # a slow or unreachable subscriber never delays
                    # the processing of other messages.
                    task = asyncio.create_task(
                        self._deliver_with_ack(t, msg)
                    )
                    self._rpc_tasks.add(task)
                    task.add_done_callback(self._rpc_tasks.discard)
                else:
                    targets.append(t)
                    tasks.append(t.send(msg))
            elif telemetry.ENABLED and t != twin:
                telemetry.sent(
                    self, msg, t, False, _skip_reason(self, t, twin, topic)
                )

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for t, result in zip(targets, results):
                if isinstance(result, Exception):
                    self._log_delivery_failure(topic, t, result)
                elif msg.recvts is not None:
                    t.mark = msg.recvts

    async def command_permitted(self, twin) -> bool:
        """Check that `twin` is allowed to send commands.

        When the router requires authentication, a connected component that
        has not authenticated is disconnected and `False` is returned.
        Anonymous components connected from the loopback interface are
        always allowed.
        """
        if (
            self.authenticated
            and twin.inbound
            and not twin.isauth
            and not self._is_loopback(twin)
        ):
            logger.warning("[%s] command from unauthenticated [%s]", self, twin)
            await twin.close()
            return False
        return True

    async def _pubsub_msg(self, msg: rp.PubSubMsg):
        twin = msg.twin
        if not await self.command_permitted(twin):
            return

        # An invalid publish is dropped without ack. A wildcard topic is
        # the form of a delivered match, so it is invalid only when received
        # from a connected component.
        if rp.invalid_topic(msg.topic) or ("*" in msg.topic and twin.inbound):
            logger.warning("[%s] dropping publish with invalid topic", twin)
            return

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

    def _select_twin(self, tenant, topic, twins=None):
        """Apply the configured load-balancing policy to pick an implementor.

        Only open twins belonging to `tenant` (the requestor's tenant) are
        eligible. `twins` defaults to `self.exposers[topic]` but callers performing
        mesh-wide routing (see `find_implementor`) pass an already-filtered
        candidate list, e.g. with the requestor removed to avoid loopbacks.
        """
        if twins is None:
            twins = self.exposers[topic]
        if self.policy == Policy.FIRST_UP:
            return first_up(tenant, twins)
        elif self.policy == Policy.ROUND_ROBIN:
            return round_robin(self, tenant, topic, twins)
        elif self.policy == Policy.LESS_BUSY:
            return less_busy(tenant, twins)
        return None

    def find_implementor(self, requestor, topic: str):
        """Pick the twin that should execute an RPC request for `topic`.

        Mirrors Rembus.jl's `find_implementor`/`select_twin`: `topic_impls`
        (here `self.exposers`) is populated mesh-wide by `EXPOSE_CMD`/
        `UNEXPOSE_CMD` propagation (see `rembus.admin.mark_and_broadcast`),
        so the implementor returned may be a local component or the twin
        representing a neighbor broker, in which case the request simply
        keeps hopping: the neighbor router receives it as an ordinary
        `RpcReqMsg` and repeats this same lookup against its own
        `exposers` table.

        Returns a tuple `(twin, status)`: `status` is `rp.STS_OK` when a
        twin was found, `rp.STS_METHOD_LOOPBACK` when the only candidate
        was the requestor itself (the call would otherwise call itself
        through a loopback route), or `rp.STS_METHOD_UNAVAILABLE` when no
        implementor is currently reachable.
        """
        implementors = self.exposers.get(topic, [])
        candidates = [t for t in implementors if t.rid != requestor.rid]
        if not candidates:
            if implementors:
                # The only implementor was the requestor itself.
                return None, rp.STS_METHOD_LOOPBACK
            return None, rp.STS_METHOD_UNAVAILABLE

        target_twin = self._select_twin(requestor.tenant, topic, candidates)
        if target_twin is None:
            return None, rp.STS_METHOD_UNAVAILABLE

        return target_twin, rp.STS_OK

    def local_exports(self):
        """Return this router's own directly-registered topics.

        Splits the non-builtin keys of `self.handler` into
        `(exposers, subscribers)`, using `self.local_subscriber` to tell
        a locally-subscribed topic apart from a locally-exposed one.

        Mirrors Rembus.jl's `twin_configuration`: used to rebuild the
        `SETUP_CMD` payload a twin replays to its upstream broker after a
        reconnection (see `Twin.setup`), since that broker may have lost
        all prior incremental `expose`/`subscribe` state for the link.
        """
        exposers = [
            topic
            for topic in self.handler
            if topic not in BUILTIN_TOPICS and topic not in self.local_subscriber
        ]
        subscribers = list(self.local_subscriber)
        return exposers, subscribers

    def _topics(self, results: set, target, topic_map: dict) -> list:
        """Extend `results` with every key of `topic_map` (`self.exposers`
        or `self.subscribers`) that has at least one twin other than
        `target` associated with it, then return it as a sorted list.

        Mirrors Rembus.jl's `_topics`: excluding `target` ensures a peer
        is never told about its own topic, relevant when the attestation
        response is computed for the very twin whose link is being
        (re)established.
        """
        for topic, twins in topic_map.items():
            if any(t.rid != target.rid for t in twins):
                results.add(topic)
        return sorted(results)

    def topic_impls(self, target) -> list:
        """Topics `target` should be told this router implements (or
        knows an implementor for): local non-builtin, non-subscriber
        handlers (see `local_exports`) unioned with every topic in
        `self.exposers` reachable through some twin other than `target`.

        Used to build the exports sent back in the attestation response,
        see `get_topics`.
        """
        exposers, _ = self.local_exports()
        return self._topics(set(exposers), target, self.exposers)

    def topic_interests(self, target) -> list:
        """Topics `target` should be told this router (or some twin
        reachable through it) is subscribed to: local subscriber
        callbacks (see `local_exports`) unioned with every topic in
        `self.subscribers` reachable through some twin other than
        `target`.

        Used to build the exports sent back in the attestation response,
        see `get_topics`.
        """
        _, subscribers = self.local_exports()
        return self._topics(set(subscribers), target, self.subscribers)

    def get_topics(self, target) -> list:
        """Return `[topic_impls(target), topic_interests(target)]`: this
        router's own exports view, sent back to `target` as the
        attestation response payload (see `_handle_attestation`/
        `_auth_identity`) and applied by the peer with `update_tables`.
        """
        return [self.topic_impls(target), self.topic_interests(target)]

    def update_tables(self, twin, exports) -> None:
        """Register `exports` (the `[topic_impls, topic_interests]` pair
        returned by the peer's `get_topics` in the attestation response)
        into this router's own mesh routing tables for `twin`.

        Lets this router immediately forward RPC requests and Pub/Sub
        messages towards `twin` without waiting for individual
        `expose`/`subscribe` admin commands to propagate. No-op if
        `exports` is falsy or `ismultipath` gates it off.
        """
        if not exports or not ismultipath(self):
            return

        implementors, interests = exports
        for topic in implementors:
            add_exposer(self, twin, topic)
        for topic in interests:
            add_subscriber(self, twin, topic, rp.Now)

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

    async def _run_local_rpc_handler(
        self, msg: rp.RpcReqMsg, topic: str, data
    ) -> None:
        """`await self.evaluate(...)` for a locally-exposed RPC handler
        and send its response, out-of-line from `_task_impl`'s
        dispatch loop (see `_rpcreq_msg`'s local-handler branch for
        why)."""
        status = rp.STS_OK
        try:
            output = await self.evaluate(msg.twin, topic, data)
        except Exception as e:  # pylint: disable=broad-exception-caught
            status = rp.STS_METHOD_EXCEPTION
            output = f"{e}"
            logger.error("%s exception: %s", msg.twin, e, exc_info=True)
        try:
            outmsg = rp.ResMsg(
                id=msg.id, status=status, data=rp.df2tag(output)
            )
            await msg.twin.send(outmsg)
        except (TypeError, ValueError, cbor2.CBOREncodeError) as e:
            # the return value cannot be encoded: answer with a failure
            # instead of leaving the caller waiting for the timeout
            logger.error("%s response encoding failed: %s", msg.twin, e)
            outmsg = rp.ResMsg(
                id=msg.id, status=rp.STS_METHOD_EXCEPTION, data=f"{e}"
            )
            await msg.twin.send(outmsg)

    async def _rpcreq_msg(self, msg: rp.RpcReqMsg):
        """Handle an RPC request."""
        data = rp.tag2df(msg.data)
        logger.debug("[%s] rpc: %s", self, msg)
        topic = msg.topic
        if not await self.command_permitted(msg.twin):
            return
        if not topic or "*" in topic or (
            topic.startswith("__") and topic.endswith("__")
        ):
            await msg.twin.send(
                rp.ResMsg(
                    id=msg.id, status=rp.STS_GENERIC_ERROR, data="invalid topic"
                )
            )
            return
        if msg.twin.isbroker():
            await self.send_message(msg)
        elif not self.isauthorized(topic, msg.twin):
            # indistinguishable from an unknown method
            outmsg = rp.ResMsg(
                id=msg.id, status=rp.STS_METHOD_NOT_FOUND, data=topic
            )
            await msg.twin.send(outmsg)
        elif (
            msg.target is not None
            and msg.target != self.id
            and not self.relay
        ):
            outmsg = rp.ResMsg(
                id=msg.id,
                status=rp.STS_GENERIC_ERROR,
                data="direct not permitted in server mode",
            )
            await msg.twin.send(outmsg)
        elif msg.target is not None and msg.target != self.id:
            # the target is a bare component id matched by exact equality
            candidates = [
                t for t in self.id_twin.values() if t.rid == msg.target
            ]
            target_twin = next(
                (t for t in candidates if t.isopen()),
                candidates[0] if candidates else None,
            )
            if target_twin and target_twin.isopen():
                logger.debug("[%s] sending to [%s]", self, target_twin)
                # dispatch to the target twin, reset target
                msg.target = None
                await target_twin.inbox.put(msg)
            else:
                status = (
                    rp.STS_TARGET_DOWN
                    if target_twin
                    else rp.STS_TARGET_NOT_FOUND
                )
                outmsg = rp.ResMsg(
                    id=msg.id, status=status, data=msg.target
                )
                await msg.twin.send(outmsg)
        elif topic in self.handler:
            # Run the local handler as an independent task rather than
            # `await`ing it inline: this coroutine runs on
            # `_task_impl`'s single-consumer dispatch loop, which is
            # also responsible for delivering every other queued
            # message -- including, for example, a pub/sub response
            # that a locally RPC-exposed handler itself blocks on
            # waiting for. Awaiting the handler here would therefore
            # self-deadlock: the router could never dispatch the very
            # response the handler is waiting on until the handler
            # itself returns (by timing out). Dispatching it instead
            # lets the loop immediately go back to `inbox.get()` and
            # process whatever is queued next, while this task runs
            # concurrently and sends its response whenever it
            # completes.
            task = asyncio.create_task(
                self._run_local_rpc_handler(msg, topic, data)
            )
            self._rpc_tasks.add(task)
            task.add_done_callback(self._rpc_tasks.discard)
        elif (
            topic in self.exposers
            and (self.relay or msg.twin.isbroker())
            and self.isauthorized(topic, msg.twin)
        ):
            target_twin, sts = self.find_implementor(msg.twin, topic)
            logger.debug("[%s] target twin: %s", self, target_twin)
            if target_twin is None:
                outmsg = rp.ResMsg(id=msg.id, status=sts, data=topic)
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

    async def _await_attestation(self, msg: rp.IdentityMsg) -> None:
        """Defer the response to an Identity received while a challenge
        is pending.

        The pending challenge stays valid: the Identity response carries
        the outcome of the Attestation (no data), or a generic error if
        the Attestation is not received within `request_timeout`.
        """
        twin = msg.twin
        outcome: asyncio.Future = asyncio.get_running_loop().create_future()

        def _set(sts: int) -> None:
            if not outcome.done():
                outcome.set_result(sts)

        twin.handler["att"] = _set
        try:
            sts = await asyncio.wait_for(
                outcome, self.config.request_timeout
            )
        except asyncio.TimeoutError:
            sts = rp.STS_GENERIC_ERROR
        finally:
            if twin.handler.get("att") is _set:
                twin.handler.pop("att")
        try:
            await twin.response(sts, msg)
        except websockets.ConnectionClosed:
            logger.debug("[%s] closed before the identity response", twin)

    async def _handle_identity(self, msg: rp.IdentityMsg) -> None:
        if "challenge" in msg.twin.handler and not self._anonymous_ok(msg):
            # Run as a task: the router loop must keep dispatching the
            # Attestation this Identity waits for.
            task = asyncio.create_task(self._await_attestation(msg))
            self._rpc_tasks.add(task)
            task.add_done_callback(self._rpc_tasks.discard)
            return

        if msg.twin.uid.hasname:
            # The connection is already identified.
            await msg.twin.response(
                rp.STS_GENERIC_ERROR, msg, "already identified"
            )
            return

        twin_id = msg.cid
        if not valid_cid(twin_id):
            await msg.twin.response(rp.STS_GENERIC_ERROR, msg, "invalid cid")
            return

        # twins are keyed by cid and the netlink of the listening endpoint
        prior = self.id_twin.get(f"{twin_id}@{msg.twin.uid.netlink}")
        if prior is not None and prior.isopen():
            if self.config.overwrite_connection and prior is not msg.twin:
                logger.debug(
                    "[%s] closing prior connection of [%s]", self, twin_id
                )
                prior.overwritten = True
                await prior.close()
            else:
                logger.warning(
                    "[%s] node with id [%s] is already connected",
                    self,
                    twin_id,
                )
                await msg.twin.response(rp.STS_GENERIC_ERROR, msg, "already connected")
                return

        logger.debug("[%s] identity: %s", self, msg.cid)
        await self._auth_identity(msg)

    async def _handle_attestation(self, msg: rp.AttestationMsg) -> None:
        if not valid_cid(msg.cid):
            await msg.twin.response(rp.STS_GENERIC_ERROR, msg, "invalid cid")
            return
        if msg.twin.uid.hasname or "challenge" not in msg.twin.handler:
            # Already identified or no pending challenge.
            await msg.twin.response(
                rp.STS_GENERIC_ERROR, msg, "unexpected attestation"
            )
            return
        sts = await self._verify_signature(msg)
        data = self.get_topics(msg.twin) if sts == rp.STS_OK else None
        await msg.twin.response(sts, msg, data)
        notify = msg.twin.handler.pop("att", None)
        if notify is not None:
            notify(sts)
        if sts != rp.STS_OK:
            # Give the error response time to reach the peer, then close.
            task = asyncio.create_task(self._close_after(msg.twin, 0.5))
            self._rpc_tasks.add(task)
            task.add_done_callback(self._rpc_tasks.discard)

    async def _close_after(self, twin, delay: float) -> None:
        """Close `twin` after `delay` seconds."""
        await asyncio.sleep(delay)
        await twin.close()

    async def _handle_admin(self, msg: rp.AdminMsg) -> None:
        logger.debug("[%s] admin: %s", self, msg)
        if not await self.command_permitted(msg.twin):
            return
        if msg.twin.isbroker():
            await self.send_message(msg)
        else:
            await admin_command(msg)

    async def _periodic_saver(self, interval: float = 1.0):
        """Push periodic save messages into the inbox."""
        while True:
            await asyncio.sleep(interval)
            await self.inbox.put("save_messages")

    async def _save_data_at_rest_in_background(self) -> None:
        """Run `rdb.save_data_at_rest` in a worker thread so the
        (synchronous, potentially multi-second under real traffic)
        DuckLake write never blocks this router's single pub/sub
        dispatch loop (`_task_impl`). Only one such thread runs at a
        time; see `self._data_at_rest_task`.

        Uses a dedicated `self.db.cursor()` rather than the raw
        `self.db` connection: DuckDB connections are not safe for
        concurrent use from multiple threads, and the main event-loop
        thread may still issue other queries against `self.db` while
        this background thread is writing (e.g. a `Twin`/`Router`
        shutdown sequence, or application cursor-based DB
        accessors). Sharing the raw connection across threads can
        leave it permanently stuck (persistent `database is locked`
        errors no amount of retrying resolves), whereas a `cursor()`
        is an independent, thread-safe handle onto the same
        database."""
        try:
            await asyncio.to_thread(
                rdb.save_data_at_rest, self, self.db.cursor()
            )
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("[%s] background data-at-rest save failed", self)

    async def _task_impl(self) -> None:
        """Switch messages between twins."""
        logger.debug("[%s] router started", self)
        while True:
            msg = await self.inbox.get()
            # Time how long this single dispatch-loop iteration takes
            # to fully `await` to completion. This loop processes
            # exactly one queued message at a time (dequeuing the next
            # only after the current one's handler has fully
            # returned), so any case that's slow to `await` here
            # delays delivery of every other message already queued
            # behind it. Kept as a permanent early-warning signal
            # (this exact symptom -- a locally RPC-exposed handler
            # blocked waiting on a pub/sub response only this same
            # loop could ever deliver -- is what caused the
            # self-deadlock fixed by `_run_local_rpc_handler` below).
            _dispatch_t0 = time.monotonic()
            _dispatch_label = (
                msg.topic if isinstance(msg, rp.PubSubMsg) else msg
            )
            match msg:
                case "shutdown":
                    logger.debug("[%s] router shutting down", self)
                    if (
                        self._data_at_rest_task is not None
                        and not self._data_at_rest_task.done()
                    ):
                        await self._data_at_rest_task
                    rdb.save_data_at_rest(self)
                    break
                case "save_messages":
                    # Save cached messages to the database periodically.
                    # Offload to a background thread (fire-and-forget,
                    # at most one in flight) so this synchronous
                    # DuckLake write never delays delivery of other
                    # queued messages (e.g. a pending command
                    # response) behind it.
                    if (
                        self._data_at_rest_task is None
                        or self._data_at_rest_task.done()
                    ):
                        self._data_at_rest_task = asyncio.create_task(
                            self._save_data_at_rest_in_background()
                        )
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
                case rp.RekeyMsg():
                    await self._rekey_node(msg)
                case rp.OpaqueMsg():
                    await self._opaque_register(msg)
                case rp.UnregisterMsg():
                    await self._unregister_node(msg)
            # Log any dispatch iteration that took a suspiciously long
            # time (see note above): pinpoints exactly which queued
            # message/case is holding up this loop, should some other
            # slow-to-`await` case be introduced in the future.
            _dispatch_elapsed = time.monotonic() - _dispatch_t0
            if _dispatch_elapsed > 0.5:
                logger.warning(
                    "[%s] router dispatch of %r took %.3fs "
                    "(blocked delivery of anything queued behind it)",
                    self,
                    _dispatch_label,
                    _dispatch_elapsed,
                )

    async def evaluate(self, twin, topic: str, data: Any) -> Any:
        """Invoke the handler associate with the message topic."""
        if self.shared is not None:
            if "repl" in self.id_twin:
                target = self.id_twin["repl"]
            else:
                target = twin
                
            output = await get_response(
                self.handler[topic](
                    *getargs(data),
                    ctx=self.shared,
                    node=target,
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
        url.port = self.wsport
        twin = WsTwin(url, bottom_router(self), False)
        await twin.start()
        self.id_twin[url.twkey] = twin

        twin.socket = ws
        twin.inbound = True
        challenge_task = None
        if self.authenticated:
            await twin.send(self._challenge(rp.CONNECTION_ID, twin))
            if not self._is_loopback(twin):
                challenge_task = asyncio.create_task(
                    self._challenge_timeout(twin)
                )
        try:
            await twin.twin_receiver()
        finally:
            if challenge_task:
                challenge_task.cancel()

    async def _challenge_timeout(self, twin):
        """Disconnect the twin if it does not authenticate in time."""
        await asyncio.sleep(self.config.challenge_timeout)
        if not twin.isauth:
            logger.warning("[%s] authentication timeout", twin)
            await twin.close()

    async def serve_ws(self, port: int, issecure: bool = False):
        """Start a WebSocket server to handle incoming connections."""
        self.wsport = port
        self.wsprotocol = "wss" if issecure else "ws"
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
            ping_interval=None,  # only clients send pings
            # per-frame limit only; the reassembled message is not bounded
            max_size=(None, self.config.max_ws_frame),
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

    async def _update_twin(self, twin, identity, meta: Optional[dict] = None):
        logger.debug("[%s] setting name: [%s]", twin, identity)
        self.id_twin.pop(twin.twkey, twin)
        twin.rid = identity
        self.id_twin[twin.twkey] = twin
        await twin_up(twin)
        if twin.db is not None:
            load_twin(twin)
            if twin.router.upstream:
                await twin.router.upstream.setup_twin(twin)

        if meta:
            self.network.extend(
                mesh_nodes(identity, self._remote_host(twin), meta)
            )

    def _is_loopback(self, twin) -> bool:
        """Return True if the twin is connected from the loopback interface."""
        try:
            ip = ipaddress.ip_address(self._remote_host(twin))
        except ValueError:
            return False
        if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
            ip = ip.ipv4_mapped
        return ip.is_loopback

    def _remote_host(self, twin) -> str:
        """Return the remote host address of the connecting twin's socket."""
        addr = getattr(twin.socket, "remote_address", None)
        return addr[0] if addr else "unknown"

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

            await self._update_twin(twin, msg.cid, msg.meta)
            twin.isauth = True
            return rp.STS_OK
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error("verification failed: %s (%s)", e, type(e))
            return rp.STS_GENERIC_ERROR

    def _challenge(self, msgid: int, twin):
        """Generate a challenge for the identity authentication."""
        challenge_val = os.urandom(4)
        twin.handler["challenge"] = lambda twin: challenge_val
        return rp.ResMsg(
            id=msgid,
            status=rp.STS_CHALLENGE,
            data=bytes_to_b64(challenge_val, twin.enc),
        )

    def _anonymous_ok(self, msg: rp.IdentityMsg) -> bool:
        """A loopback component that is not provisioned may stay anonymous."""
        return self._is_loopback(msg.twin) and not self._needs_auth(msg.cid)

    async def _auth_identity(self, msg: rp.IdentityMsg):
        """Authenticate the identity of the component."""
        twin = msg.twin
        identity = msg.cid

        if not self._anonymous_ok(msg) and (
            self.authenticated or self._needs_auth(identity)
        ):
            # Component is provisioned (or the router requires
            # authentication), send the challenge
            response = self._challenge(msg.id, twin)
        else:
            await self._update_twin(twin, identity, msg.meta)
            response = rp.ResMsg(
                id=msg.id, status=rp.STS_OK, data=self.get_topics(twin)
            )

        await twin.send(response)

    def _resolve_secret(self, cid: str, mode: str):
        """Resolve the registration secret of `cid` (REQ-AUTH-065/066/068).

        The secret of the cid if it has one, otherwise the secret of its
        tenant; there is no fallback after a cid secret is found. Returns
        ``(scope, value)`` or None when no secret applies in `mode`.
        """
        tenant = tenant_of(cid, self.config.tenants_enabled)
        if cid in self.cid_pins:
            scope, kind, value = "cid", MODE_PIN, self.cid_pins[cid]
        elif cid in self.cid_opaque:
            scope, kind, value = "cid", MODE_OPAQUE, self.cid_opaque[cid]
        elif tenant in self.tenants:
            scope, kind, value = "tenant", MODE_PIN, self.tenants[tenant]
        elif tenant in self.opaque_tenants:
            scope = "tenant"
            kind, value = MODE_OPAQUE, self.opaque_tenants[tenant]
        else:
            return None
        explicit = self.tenant_modes.get(tenant)
        if kind != mode or (explicit is not None and explicit != mode):
            return None
        return scope, value

    def _issue_nonce(self, twin, op: int, cid: str) -> bytes | None:
        """Issue a single-use nonce bound to the connection, op and cid.

        The secret resolved now is remembered: the exchange fails if it is
        not the one resolved at the final step (REQ-AUTH-071).
        """
        now = time.monotonic()
        nonces = twin.pin_nonces
        for key in [k for k, v in nonces.items() if v[1] <= now]:
            del nonces[key]
        if (op, cid) not in nonces and len(nonces) >= MAX_PIN_NONCES:
            return None
        nonce = os.urandom(PIN_NONCE_SIZE)
        nonces[(op, cid)] = (
            nonce,
            now + PIN_NONCE_TTL,
            self._resolve_secret(cid, MODE_PIN),
        )
        return nonce

    def _get_token(self, twin, op: int, msg) -> bytes | None:
        """Check the pin proof of a Register/Rekey proof request.

        The outstanding nonce is discarded whatever the outcome.
        """
        tenant = tenant_of(msg.cid, self.config.tenants_enabled)
        resolved = self._resolve_secret(msg.cid, MODE_PIN)
        entry = twin.pin_nonces.pop((op, msg.cid), None)
        valid = (
            entry is not None
            and isinstance(msg.pin, bytes)
            and entry[1] > time.monotonic()
            and resolved is not None
            and entry[2] == resolved
            and isinstance(msg.pubkey, bytes)
            and isinstance(msg.type, int)
            and not isinstance(msg.type, bool)
        )
        if valid:
            expected = rp.pin_proof(
                resolved[1], msg.cid, entry[0], msg.pubkey, msg.type
            )
            valid = hmac.compare_digest(expected, msg.pin)
        if not valid:
            logger.warning("tenant %s: invalid token", tenant)
            return None
        logger.debug("tenant %s: token is valid", tenant)
        return msg.pin

    async def _pin_challenge(self, twin, op: int, msg) -> None:
        """Answer a challenge request with a nonce (status 11)."""
        nonce = self._issue_nonce(twin, op, msg.cid)
        if nonce is None:
            await twin.response(rp.STS_GENERIC_ERROR, msg, "too many nonces")
        else:
            await twin.send(
                rp.ResMsg(
                    id=msg.id,
                    status=rp.STS_CHALLENGE,
                    data=bytes_to_b64(nonce, twin.enc),
                )
            )

    @staticmethod
    def _pin_step(msg) -> str:
        """Classify the pin of a Register/Rekey: challenge, proof or invalid."""
        if not isinstance(msg.pin, bytes) or len(msg.pin) not in (0, 32):
            return "invalid"
        return "challenge" if len(msg.pin) == 0 else "proof"

    async def _register_node(self, msg: rp.RegisterMsg):
        """Provision a new node."""
        sts = rp.STS_GENERIC_ERROR
        reason = None
        twin = msg.twin
        if not valid_cid(msg.cid):
            await twin.response(sts, msg, "invalid cid")
            return
        step = self._pin_step(msg)
        if step == "challenge":
            await self._pin_challenge(twin, rp.TYPE_REGISTER, msg)
            return
        # Any proof request consumes the nonce, even a malformed one.
        token = self._get_token(twin, rp.TYPE_REGISTER, msg)
        try:
            if step == "invalid":
                reason = "invalid pin length"
            elif token is None:
                reason = "wrong tenant/pin"
            elif rp.isregistered(self.id, msg.cid):
                sts = rp.STS_NAME_ALREADY_TAKEN
                reason = f"[{msg.cid}] not available"
            else:
                rp.validate_pubkey(msg.pubkey, msg.type)
                kdir = rs.keys_dir(self.id)
                os.makedirs(kdir, exist_ok=True)
                rp.save_pubkey(self.id, msg.cid, msg.pubkey, msg.type)
                sts = rp.STS_OK
                logger.debug("cid %s registered", msg.cid)
        except ValueError as e:
            reason = str(e)
        finally:
            await twin.response(sts, msg, reason)

    async def _opaque_register(self, msg: rp.OpaqueMsg):
        """Register a node authorized by an OPAQUE login to the tenant."""
        # Imported here to keep the dependency optional at import time.
        # pylint: disable-next=import-outside-toplevel
        import opaque_ke_py as ok

        # pylint: disable-next=import-outside-toplevel
        from . import opaque

        twin = msg.twin
        sts = rp.STS_GENERIC_ERROR
        data = None
        tenant = tenant_of(msg.cid, self.config.tenants_enabled)
        rekey = msg.op == rp.OPAQUE_OP_REKEY
        try:
            if not valid_cid(msg.cid) or msg.op not in (
                rp.OPAQUE_OP_REGISTER,
                rp.OPAQUE_OP_REKEY,
            ):
                data = "invalid opaque request"
            elif rekey and not self._rekey_session_ok(twin, msg.cid):
                data = "invalid operation"
            elif msg.stage == rp.OPAQUE_START:
                resolved = self._resolve_secret(msg.cid, MODE_OPAQUE)
                if self._opaque_setup is None:
                    self._opaque_setup = opaque.load_setup(self.id)
                if resolved is None:
                    # Same reply as for an existing record: the exchange
                    # fails at the finish stage (no enumeration oracle).
                    record = opaque.fake_record(self.id)
                    identifier = tenant
                else:
                    scope, record = resolved
                    identifier = (
                        opaque.cid_identifier(msg.cid)
                        if scope == "cid"
                        else tenant
                    )
                states = twin.handler.setdefault("opaque", {})
                now = time.monotonic()
                for k in [k for k, v in states.items() if v[1] <= now]:
                    del states[k]
                key = (msg.cid, msg.op)
                if key not in states and len(states) >= OPAQUE_MAX_EXCHANGES:
                    data = "too many opaque exchanges"
                else:
                    out = ok.server_login_start(
                        self._opaque_setup,
                        record,
                        bytes(msg.data),
                        identifier.encode(),
                    )
                    states[key] = (out.get_state(), now + OPAQUE_TTL, resolved)
                    data = bytes_to_b64(out.get_message(), twin.enc)
                    sts = rp.STS_OK
            elif msg.stage == rp.OPAQUE_FINISH:
                # A login state is valid for one finalization only.
                entry = twin.handler.get("opaque", {}).pop(
                    (msg.cid, msg.op), None
                )
                if entry is None or entry[1] <= time.monotonic():
                    data = "unexpected opaque message"
                else:
                    sts, data = self._opaque_finish(
                        msg, entry[0], ok, opaque, entry[2]
                    )
            else:
                data = "invalid opaque stage"
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("[%s] opaque register: %s", twin, e)
            sts = rp.STS_GENERIC_ERROR
            data = "opaque register failed"
        finally:
            await twin.response(sts, msg, data)

    @staticmethod
    def _rekey_session_ok(twin, cid: str) -> bool:
        """An OPAQUE Rekey needs a connection authenticated as `cid`."""
        return bool(twin.isauth and twin.rid == cid)

    def _opaque_finish(self, msg: rp.OpaqueMsg, state, ok, opaque, resolved):
        """Verify the OPAQUE login and store the public key.

        `resolved` is the secret resolved at stage 1: it must still be the
        one that applies (REQ-AUTH-071). Returns the response status and
        reason.
        """
        err = rp.STS_GENERIC_ERROR
        try:
            session_key = ok.server_login_finish(
                state, bytes(msg.data)
            ).get_session_key()
        except Exception:  # pylint: disable=broad-exception-caught
            return err, "wrong tenant/pin"
        if resolved is None or resolved != self._resolve_secret(
            msg.cid, MODE_OPAQUE
        ):
            return err, "wrong tenant/pin"

        pubkey = msg.pubkey
        if not isinstance(pubkey, bytes) or not isinstance(msg.type, int):
            return err, "invalid public key"
        expected = opaque.key_mac(
            session_key, msg.cid, msg.type, pubkey, msg.op
        )
        if not isinstance(msg.mac, bytes) or not hmac.compare_digest(
            expected, msg.mac
        ):
            return err, "wrong tenant/pin"
        rekey = msg.op == rp.OPAQUE_OP_REKEY
        if rekey and not self._rekey_session_ok(msg.twin, msg.cid):
            return err, "invalid operation"
        try:
            rp.validate_pubkey(pubkey, msg.type)
        except ValueError as e:
            return err, str(e)
        if rekey:
            if not rp.isregistered(self.id, msg.cid):
                return err, f"[{msg.cid}] not registered"
            rp.replace_pubkey(self.id, msg.cid, pubkey, msg.type)
            logger.debug("cid %s rekeyed (opaque)", msg.cid)
            return rp.STS_OK, None
        if rp.isregistered(self.id, msg.cid):
            return rp.STS_NAME_ALREADY_TAKEN, f"[{msg.cid}] not available"
        os.makedirs(rs.keys_dir(self.id), exist_ok=True)
        rp.save_pubkey(self.id, msg.cid, pubkey, msg.type)
        logger.debug("cid %s registered (opaque)", msg.cid)
        return rp.STS_OK, None

    async def _rekey_node(self, msg: rp.RekeyMsg):
        """Replace the public key of the authenticated component."""
        sts = rp.STS_GENERIC_ERROR
        reason = None
        twin = msg.twin
        try:
            if not valid_cid(msg.cid):
                reason = "invalid cid"
            elif not await self.command_permitted(twin) or not twin.isauth:
                reason = "invalid operation"
            elif msg.cid != twin.rid:
                reason = "cid does not match the authenticated cid"
            elif self._pin_step(msg) == "challenge":
                await self._pin_challenge(twin, rp.TYPE_REKEY, msg)
                return
            else:
                step = self._pin_step(msg)
                token = self._get_token(twin, rp.TYPE_REKEY, msg)
                if step == "invalid":
                    reason = "invalid pin length"
                elif token is None:
                    reason = "wrong tenant/pin"
                elif not rp.isregistered(self.id, msg.cid):
                    reason = f"[{msg.cid}] not registered"
                else:
                    rp.validate_pubkey(msg.pubkey, msg.type)
                    rp.replace_pubkey(self.id, msg.cid, msg.pubkey, msg.type)
                    sts = rp.STS_OK
                    logger.debug("cid %s rekeyed", msg.cid)
        except (ValueError, OSError) as e:
            sts = rp.STS_GENERIC_ERROR
            reason = str(e)
        await twin.response(sts, msg, reason)

    async def _unregister_node(self, msg: rp.UnregisterMsg):
        """Unprovisions the component."""
        sts = rp.STS_GENERIC_ERROR
        reason = None
        try:
            if (
                not await self.command_permitted(msg.twin)
                or not msg.twin.isauth
            ):
                reason = "invalid operation"
            else:
                self._unregister_cid(msg.twin.rid)
                sts = rp.STS_OK
        finally:
            await msg.twin.response(sts, msg, reason)

    def _unregister_cid(self, cid: str):
        """Remove the key and the secret of `cid` together (REQ-AUTH-074).

        On a failure to remove the key the secret is restored.
        """
        pin = self.cid_pins.pop(cid, None)
        record = self.cid_opaque.pop(cid, None)
        saved = []
        try:
            if self.db is not None and (pin or record):
                for table in ("cid_secret", "cid_opaque"):
                    saved.append(
                        (
                            table,
                            self.db.execute(
                                f"SELECT * FROM {table}"
                                " WHERE name = ? AND cid = ?",
                                (self.id, cid),
                            ).fetchall(),
                        )
                    )
                    self.db.execute(
                        f"DELETE FROM {table} WHERE name = ? AND cid = ?",
                        (self.id, cid),
                    )
            rp.remove_pubkey(self, cid)
        except BaseException:
            if pin is not None:
                self.cid_pins[cid] = pin
            if record is not None:
                self.cid_opaque[cid] = record
            for table, rows in saved:
                for row in rows:
                    self.db.execute(
                        f"INSERT INTO {table} VALUES (?, ?, ?)", tuple(row)
                    )
            raise


async def twin_up(twin):
    """Update the twin up status."""
    router = twin.router
    db = router.db
    # Get remote address
    remote_ip, _ = twin.socket.remote_address
    loop = asyncio.get_event_loop()
    try:
        remote_host = await loop.run_in_executor(
            None, lambda: socket.gethostbyaddr(remote_ip)[0]
        )
    except socket.herror:
        remote_host = remote_ip

    now = datetime.now(timezone.utc)
    db.execute(
        "DELETE FROM component WHERE broker = ? AND twin = ?",
        (router.id, twin.rid),
    )
    db.execute(
        """
        INSERT INTO component (broker, host, twin, status, last_changed)
        VALUES (?, ?, ?, ?, ?)
        """,
        (router.id, remote_host, twin.rid, "up", now),
    )


async def twin_down(twin):
    """Update the twin down status."""
    router = twin.router
    db = router.db
    now = datetime.now(timezone.utc)
    db.execute(
        """UPDATE component SET status = 'down', last_changed = ?
        WHERE broker = ? AND twin = ?""",
        (now, router.id, twin.rid),
    )


def top_router(router: Supervised) -> Router:
    """Return the topmost router (the core router)."""
    r = router
    while r.downstream is not None:
        r = r.downstream
    return cast(Router, r)


def bottom_router(router: Supervised) -> Supervised:
    """
    Return the router attached to the twin (the lowest router in the chain).
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
    logger.debug("[%s] admins: %s", router, router.admins)


def load_tenants(router):
    """Load the tenants."""
    db = router.db
    result = db.sql(
        "SELECT tenant, secret FROM tenant WHERE name = ?", params=[router.id]
    )
    router.tenants = {row[0]: row[1] for row in result.fetchall()}
    result = db.sql(
        "SELECT tenant, password_file FROM tenant_opaque WHERE name = ?",
        params=[router.id],
    )
    router.opaque_tenants = {
        row[0]: bytes(row[1]) for row in result.fetchall()
    }
    router.cid_pins = {
        row[0]: row[1]
        for row in db.sql(
            "SELECT cid, secret FROM cid_secret WHERE name = ?",
            params=[router.id],
        ).fetchall()
    }
    router.cid_opaque = {
        row[0]: bytes(row[1])
        for row in db.sql(
            "SELECT cid, password_file FROM cid_opaque WHERE name = ?",
            params=[router.id],
        ).fetchall()
    }
    router.tenant_modes = {
        row[0]: row[1]
        for row in db.sql(
            "SELECT tenant, mode FROM tenant_mode WHERE name = ?",
            params=[router.id],
        ).fetchall()
    }


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

            if twin in router.subscribers[topic]:
                router.subscribers[topic].remove(twin)

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

            if twin in router.exposers[topic]:
                router.exposers[topic].remove(twin)

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
