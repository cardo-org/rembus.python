"""Twin Class implementation."""

from __future__ import annotations
import asyncio
from contextlib import suppress
import base64
import json
import logging
import os
from typing import Callable, Any, Optional, List
import signal
import ssl
import sys
import async_timeout
import cbor2
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa, ec
import polars as pl
import websockets
from gmqtt import Client as MQTTClient
import rembus.protocol as rp
import rembus.settings as rs
import rembus.builtins as builtins
from rembus.core import (
    RbURL,
    Supervised,
    FutureResponse,
    domain,
    response_data,
    bytes_to_b64,
)
from rembus.router import bottom_router, top_router
from rembus.keyspace import KeySpaceRouter

logger = logging.getLogger(__name__)


async def init_twin(router, uid: RbURL, enc: int, isserver: bool):
    """
    Create and start a Twin for the component that connects to a server.
    """
    if uid.protocol in ["ws", "wss"]:
        cmp = WsTwin(uid, bottom_router(router), not isserver, enc)
    elif uid.protocol in ["mqtt", "mqtts"]:
        cmp = MqttTwin(uid, bottom_router(router), not isserver, enc)
    else:
        cmp = ReplTwin(uid, bottom_router(router), not isserver, enc)
    await cmp.start()
    router.id_twin[uid.twkey] = cmp
    try:
        if cmp.isrepl():
            await builtins.load_callbacks(cmp)
        else:
            if router.config.start_anyway:
                await cmp.inbox.put("reconnect")
            else:
                await cmp.connect()
    except Exception:
        await cmp.close()
        raise
    return cmp


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


def exposed_topics_for_twin(router, twin) -> List[str]:
    """
    Get list of topics exposed by this twin.
    """
    exposed = []
    for topic, twins in router.exposers.items():
        if twin in twins:
            exposed.append(topic)
    return exposed


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


def get_ssl_context():
    """Create an SSL context for secure connections."""
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ca_crt = os.getenv("HTTP_CA_BUNDLE") or rs.rembus_ca()
    if os.path.isfile(ca_crt):
        ssl_context.load_verify_locations(ca_crt)
    else:
        logger.warning("CA file not found: %s", ca_crt)
    return ssl_context


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
        return self.router.config.db_attach  # type: ignore[return-value]

    @property
    def db(self):
        """Return the database associated with this twin."""
        return self.router.db  # type: ignore[return-value]

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

    @property
    def ismqtt(self):
        """Check if twin speaks MQTT."""
        return False

    def isadmin(self):
        return self.rid in self.router.admins

    async def response(self, status: int, msg: Any, data: Any = None):
        """Send a response to the client."""
        outmsg: Any = rp.ResMsg(id=msg.id, status=status, data=data)
        logger.info(f"SENDING {outmsg}")
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
        if sys.platform != "win32":
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

    async def send(self, msg: rp.RembusMsg):
        """Send a rembus message"""

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
        """Wait for a response or a Timeout"""
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
            return response_data(response)

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

    async def private_topic(self, topic: str):
        """
        Set the specified `topic` to private.

        The component must have the admin role to change the privacy level.
        """
        await self.setting(topic, rp.PRIVATE_TOPIC)

    async def public_topic(self, topic: str):
        """
        Set the specified `topic` to public.

        The component must have the admin role to change the privacy level.
        """
        await self.setting(topic, rp.PUBLIC_TOPIC)

    async def authorize(self, component: str, topic: str):
        """
        Authorize the `component` to private `topic`.

        `self` must have the admin role for granting topic accessibility.
        """
        await self.setting(topic, rp.AUTHORIZE, {rp.CID: component})

    async def unauthorize(self, component: str, topic: str):
        """
        Unauthorize the `component` to private `topic`.

        `self` must have the admin role for removing topic accessibility.
        """
        await self.setting(topic, rp.UNAUTHORIZE, {rp.CID: component})

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
            if isinstance(self._router, KeySpaceRouter):
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
            if isinstance(self._router, KeySpaceRouter):
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


class ReplTwin(Twin):
    """
    A ReplTwin represents a virtual Rembus component when a Twin
    is not a connector.
    """

    def __init__(
        self,
        uid: RbURL,
        router: Supervised,
        isclient: bool = True,
        enc: int = rp.CBOR,
    ):
        super().__init__(uid, router, isclient, enc)

    def isrepl(self) -> bool:
        """Check if twin is a REPL"""
        return True

    def isopen(self) -> bool:
        """Check if the connection is open."""
        return any([t.isopen() for t in self.router.twins.values()])

    async def send(self, msg: rp.RembusMsg):
        """No-op send for REPL twin."""


class WsTwin(Twin):
    """
    A Twin represents a Rembus component over WebSocket,
    either as a client or server.
    """

    def __init__(
        self,
        uid: RbURL,
        router: Supervised,
        isclient: bool = True,
        enc: int = rp.CBOR,
    ):
        super().__init__(uid, router, isclient, enc)

    def isrepl(self) -> bool:
        """Check if twin is a REPL"""
        return False

    def isopen(self) -> bool:
        """Check if the connection is open."""
        if self.socket is None:
            return False

        return self.socket.state == websockets.State.OPEN

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

    async def connect(self):
        """Connect to the Rembus broker."""
        await self.wsconnect()

    async def send(self, msg: rp.RembusMsg):
        """Send a rembus message"""
        pkt = msg.to_payload(self.enc)
        await self._send(pkt)

    async def _close_socket(self):
        if self.socket:
            await self.socket.close()

        self.socket = None


class MqttTwin(Twin):
    """
    A Twin represents a Rembus component over WebSocket,
    either as a client or server.
    """

    def __init__(
        self,
        uid: RbURL,
        router: Supervised,
        isclient: bool = True,
        enc: int = rp.CBOR,
    ):
        super().__init__(uid, router, isclient, enc)

    @property
    def ismqtt(self):
        return True

    def isrepl(self) -> bool:
        """Check if twin is a REPL"""
        return False

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
            ssl_context = get_ssl_context()

        await client.connect(
            self.uid.hostname, self.uid.port or 1883, ssl=ssl_context
        )

        client.on_message = self.on_message
        client.subscribe("#", qos=1, no_local=True)
        self.socket = client

    async def connect(self):
        """Connect to the MQTT broker."""
        await self.mqttconnect()

    async def send(self, msg: rp.RembusMsg):
        """Send a rembus message"""
        # MQTT publish
        if isinstance(msg, rp.PubSubMsg):
            data = msg.data if len(msg.data) > 1 else msg.data[0]
            self.socket.publish(msg.topic, json.dumps(data))

    def isopen(self) -> bool:
        """Check if the connection is open."""
        sts = False
        if self.socket is not None and self.socket.is_connected:
            sts = True
        return sts

    async def _close_socket(self):
        if self.socket:
            await self.socket.disconnect()

        self.socket = None
