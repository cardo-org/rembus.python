"""
The twin module implements the Twin class and the function related
to the lifecycle of twins entities.
"""

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

__all__ = [
    "Twin",
    "MqttTwin",
    "ReplTwin",
    "WsTwin",
    "exposed_topics_for_twin",
    "init_twin",
    "save_twin",
]

logger = logging.getLogger(__name__)


async def init_twin(router, uid: RbURL, enc: int, isserver: bool):
    """
    Create and initialize a Twin instance for the component identified by `uid`.

    This function sets up the messaging Twin for the specified component,
    handling all necessary initialization.

    Parameters
    ----------
    router : Router
        The Rembus router to connect or interact with.
    uid : RbURL
        The unique identifier of the component.
    enc : int
        The message encoding type (e.g., `rp.CBOR` or `rp.JSON`).
    isserver : bool
        Indicates whether the Twin should operate as a server (broker)
        or as a client (connected component).

    Returns
    -------
    Twin
        The initialized Twin instance. If `isserver` is `True`, a
        ReplTwin representing the broker is returned instead of a
        connected client Twin.

    Notes
    -----
    A ReplTwin is a special Twin that represents a broker itself,
    rather than a connected client component.
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
        if cmp.isbroker():
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
        """
        Return True if the current object has administrative privileges.

        Checks whether the component name is listed among
        the broker's administrators.
        """
        return self.rid in self.router.admins

    def isbroker(self):
        """
        Return True if this object represents a server or broker handle.

        Used to identify objects that act as message brokers within the system.
        """

    def isopen(self):
        """
        Return True if the twin is currently connected.

        Checks the connection state of the underlying socket.
        """

    async def connect(self):
        """Connect to a broker."""

    async def response(self, status: int, msg: Any, data: Any = None):
        """
        Send a response message to the component managed by this Twin.

        Parameters
        ----------
        status : int
            The status code of the response.
        msg : Any
            The message content or description.
        data : Any, optional
            Optional additional data to include in the response (default is None).
        """
        outmsg: Any = rp.ResMsg(id=msg.id, status=status, data=data)
        await self.send(outmsg)

    def inject(self, data: Any):
        """
        Inject data into the router's shared context.

        Stores the provided ``data`` in the top router's shared attribute,
        initializing or updating the shared context for components.

        Parameters
        ----------
        data : Any
            The object to store in the shared context.
        """
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
        """
        Register signal handlers to gracefully shut down the component.

        On non-Windows platforms, this adds handlers for SIGINT and SIGTERM
        that schedule the asynchronous :meth:`close` method. This ensures the
        component can perform cleanup when the process is interrupted or terminated.
        """
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

        if self.uid.isbroker():
            await self._shutdown_twins()

        if self.isclient or self.uid.isbroker():
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
        """
        Return the :class:`asyncio.Future` associated with a given message ID.

        Parameters
        ----------
        msgid : int
            The identifier of the message whose future is requested.

        Returns
        -------
        asyncio.Future
            The Future object corresponding to the message ID.
        """
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
        """
        Send a message through the Rembus messaging system.

        Parameters
        ----------
        msg : :class:`rembus.protocol.RembusMsg`
            The message to send. This must be an subclass instance of
            :class:`~rembus.protocol.RembusMsg`
            (e.g., :class:`~rembus.protocol.RpcReqMsg`,
            :class:`~rembus.protocol.PubSubMsg`, etc.).
        """

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
        if self.isbroker() and self.isopen():
            await self._router.inbox.put(req)
        elif self.socket is None:
            raise rp.RembusConnectionClosed()
        else:
            task = asyncio.create_task(self.send(req))

        futreq = FutureResponse(task, data)
        self.outreq[reqid] = futreq
        return futreq

    def send_task(self, msg: rp.RpcReqMsg) -> Any:
        """
        Send an RPC request and return a future representing its response.

        This method schedules :meth:`send` as an asyncio task and registers
        the outgoing request so that the corresponding response can be
        matched using the message identifier.

        Parameters
        ----------
        msg : :class:`rembus.protocol.RpcReqMsg`
            The RPC request message to send.

        Returns
        -------
        FutureResponse
            An awaitable object that resolves when the response for the
            given message is received.
        """
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

    async def login(self):
        """
        Perform the login handshake with the remote peer.

        The method first sends an :class:`~rembus.protocol.IdentityMsg`
        identifying the component. If the remote peer replies with a
        challenge, the challenge is signed using the component’s private
        key and an :class:`~rembus.protocol.AttestationMsg` is sent to
        complete the authentication process.

        If no challenge is returned, the connection proceeds in *free mode*
        (unauthenticated access).

        Raises
        ------
        RembusError
            If the authentication exchange fails.
        RembusTimeout
            If the remote peer does not respond within the expected time.
        """
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
        """
        Publish a message to a topic.

        Parameters
        ----------
        topic : str
            The name of the topic to publish to.
        *data : Any
            Positional payload values to include in the published message.
            These are delivered to subscribers as the message content.
        **kwargs : Any
            Optional keyword arguments controlling publication behavior
            (e.g., QoS level, slot, or other transport-specific options).

        Notes
        -----
        The message is sent asynchronously and does not wait for delivery
        acknowledgment unless explicitly configured (e.g., via QoS settings).
        """
        slot = kwargs.get("slot", None)
        qos = kwargs.get("qos", rp.QOS0) & rp.QOS2
        if qos == rp.QOS0:
            msg = rp.PubSubMsg(topic=topic, data=data, slot=slot)
            msg.twin = self
            if self.isbroker() and self.isopen():
                await self._router.inbox.put(msg)
            elif self.socket is None:
                raise rp.RembusConnectionClosed()
            else:
                await self.send(msg)
        else:
            await self._qos_publish(topic, data, qos, slot)

        return None

    async def put(self, topic: str, *args: Any, **kwargs):
        """
        Publish a message to a topic namespaced with the component ID.

        This method automatically prefixes the given `topic` with the
        component's name, so that the message is sent to a
        topic unique to this component.

        Parameters
        ----------
        topic : str
            The topic name (without the component prefix) to publish to.
        *args : Any
            Positional payload values to include in the message.
        **kwargs : Any
            Optional keyword arguments passed to :meth:`publish`,
            such as QoS, slot, or other transport-specific options.

        Notes
        -----
        This is a convenience wrapper around :meth:`publish` and is
        useful for sending messages scoped to this component.
        """
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
                    self.router.config.ack_timeout
                ):
                    done = await futreq.future
                if done:
                    break
            except TimeoutError as e:
                if retries > max_retries:
                    raise rp.RembusTimeout() from e

    async def broker_setting(self, command: str, args: dict[str, Any]):
        """
        Send a broker configuration command and wait for a response.

        This method sends an administrative message targeting the broker.
        The `command` specifies the action, and `args` provides any additional
        parameters required by the specific command.

        Parameters
        ----------
        command : str
            The broker command to execute (e.g., "reactive", "unreactive").
        args : dict[str, Any]
            Additional key-value parameters to include with the command.

        Returns
        -------
        Any
            The broker's response data, decoded from the admin message.

        Raises
        ------
        RembusError
            If the broker returns an error response.
        RembusTimeout
            If no response is received within the configured timeout.
        """
        data = {rp.COMMAND: command} | args
        futreq = await self._send_message(
            lambda id: rp.AdminMsg(id=id, topic=rp.BROKER_CONFIG, data=data)
        )
        response = await self.wait_response(futreq)
        return response_data(response)

    async def setting(
        self, topic: str, command: str, args: dict[str, Any] | None = None
    ):
        """
        Send a configuration command for a specific topic and await
        the response.

        This method allows sending an administrative or configuration
        command targeting a particular topic. The `command` specifies
        the action to perform, while `args` can include additional
        parameters required by the command.

        Parameters
        ----------
        topic : str
            The topic to which the configuration command applies.
        command : str
            The command to execute (e.g., "expose", "subscribe").
        args : dict[str, Any]
            Additional key-value parameters to include with the command.

        Returns
        -------
        Any
            The broker's response data, decoded from the admin message.

        Raises
        ------
        RembusError
            If the broker returns an error response.
        RembusTimeout
            If no response is received within the configured timeout.
        """
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
        """
        Send a Remote Procedure Call (RPC) request to a specific topic.

        This method constructs and sends an RPC request with the given
        `topic` and optional positional arguments. The request
        expects a response.

        Parameters
        ----------
        topic : str
            The name of the RPC method to invoke.
        *args : Any
            Optional positional arguments to include in the RPC request.

        Returns
        -------
        FutureResponse
            A future-like object representing the pending RPC response.

        Raises
        ------
        RembusError
            If the RPC call results in an error response.
        RembusTimeout
            If no response is received within the configured timeout.
        """
        data = rp.df2tag(args)
        futreq = await self._send_message(
            lambda id: rp.RpcReqMsg(id=id, topic=topic, data=data)
        )
        response = await self.wait_response(futreq)
        return response_data(response)

    async def direct(self, target: str, topic: str, *args: Any):
        """
        Send a direct RPC request to a specific target component.

        This method sends a Remote Procedure Call (RPC) to a single
        component identified by `target`. The `topic` specifies the
        RPC method or event, and optional positional arguments are
        passed as parameters to the request.

        Parameters
        ----------
        target : str
            The component name of the node receiving the RPC.
        topic : str
            The RPC method to invoke on the target.
        *args : Any
            Optional positional arguments to include in the RPC request.

        Returns
        -------
        FutureResponse
            A future-like object representing the pending response
            from the target.

        Raises
        ------
        RembusError
            If the target returns an error response.
        RembusTimeout
            If no response is received within the configured timeout.
        """
        data = rp.df2tag(args)
        futreq = await self._send_message(
            lambda id: rp.RpcReqMsg(
                id=id, topic=topic, target=target, data=data
            )
        )
        response = await self.wait_response(futreq)
        return response_data(response)

    async def register(self, rid: str, pin: str, scheme: int = rp.SIG_RSA):
        """
        Provision a new component with the specified identifier.

        This method registers a component in the Rembus system by
        associating it with a unique component name (`rid`) and a
        secret (`pin`) for authentication. A cryptographic key pair is generated
        according to the specified `scheme` and the public key is
        sent to the broker for registration.

        Parameters
        ----------
        rid : str
            The unique identifier for the component to register.
        pin : str
            A PIN code used for component authentication.
        scheme : int, optional
            The cryptographic signature scheme to use for key generation.
            Supported values are:
            - `rp.SIG_RSA` (default)
            - `rp.SIG_ECDSA`

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If the broker rejects the registration.
        RembusTimeout
            If no response is received within the configured timeout.
        """
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
        """
        Unregister and unprovision the current component.

        This method removes the component's registration from the Rembus
        broker and deletes any associated public key. After calling this
        method, the component loses its provisioned privileges; other
        components may then connect using the same identifier.

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If the broker rejects the unregistration request.
        RembusTimeout
            If no response is received within the configured timeout.
        """
        futreq = await self._send_message(lambda id: rp.UnregisterMsg(id=id))
        response = await self.wait_response(futreq)
        with suppress(FileNotFoundError):
            os.remove(os.path.join(rs.rembus_dir(), self.uid.id, ".secret"))
        return response_data(response)

    async def reactive(self):
        """
        Enable reactive mode for the component.

        In reactive mode, the component receives messages published on topics
        it has subscribed to.

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If the broker rejects the reactive request.
        RembusTimeout
            If no response is received within the configured timeout.
        """
        if self.isclient:
            await self.broker_setting("reactive", {"status": True})
        return self

    async def unreactive(self):
        """
        Disable reactive mode for the component.

        In unreactive mode, the component stops receiving messages published
        on topics it has subscribed to. This effectively pauses automatic
        message delivery, though the component can still publish messages or
        query the broker directly.

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If the broker rejects the unreactive request.
        RembusTimeout
            If no response is received within the configured timeout.
        """
        await self.broker_setting("reactive", {"status": False})
        return self

    async def private_topic(self, topic: str):
        """
        Mark a topic as private.

        This sets the specified `topic` to private, restricting access
        so that only authorized components can publish or subscribe to it.
        The component must have administrative privileges to change the privacy
        level of a topic.

        Parameters
        ----------
        topic : str
            The name of the topic to set as private.

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If the broker rejects the request or the component lacks privileges.
        RembusTimeout
            If no response is received within the configured timeout.
        """
        await self.setting(topic, rp.PRIVATE_TOPIC)

    async def public_topic(self, topic: str):
        """
        Mark a topic as public.

        This sets the specified `topic` to public, allowing any component
        to publish or subscribe to it. The component must have administrative
        privileges to change the privacy level of a topic.

        Parameters
        ----------
        topic : str
            The name of the topic to set as public.

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If the broker rejects the request or the component lacks privileges.
        RembusTimeout
            If no response is received within the configured timeout.
        """
        await self.setting(topic, rp.PUBLIC_TOPIC)

    async def authorize(self, component: str, topic: str):
        """
        Grant a component access to a private topic.

        This method authorizes the specified `component` to publish or subscribe
        to the private `topic`. The calling component must have administrative
        privileges to grant access.

        Parameters
        ----------
        component : str
            The identifier of the component to authorize.
        topic : str
            The name of the private topic for which access is granted.

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If the broker rejects the authorization or the caller lacks privileges.
        RembusTimeout
            If no response is received within the configured timeout.
        """
        await self.setting(topic, rp.AUTHORIZE, {rp.CID: component})

    async def unauthorize(self, component: str, topic: str):
        """
        Revoke a component's access to a private topic.

        This method removes the specified `component`'s permission to publish or
        subscribe to the private `topic`. The calling component must have
        administrative privileges to revoke access.

        Parameters
        ----------
        component : str
            The identifier of the component whose access is being revoked.
        topic : str
            The name of the private topic from which access is removed.

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If the broker rejects the revocation or the caller lacks privileges.
        RembusTimeout
            If no response is received within the configured timeout.
        """
        await self.setting(topic, rp.UNAUTHORIZE, {rp.CID: component})

    async def subscribe(
        self,
        fn: Callable[..., Any],
        msgfrom: float = rp.Now,
        topic: Optional[str] = None,
    ):
        """
        Subscribe a callback function to a topic to receive published messages.

        The callback `fn` will be invoked for each message published to the
        specified `topic`. Messages published before the subscription can
        optionally be ignored using the `msgfrom` parameter.

        Parameters
        ----------
        fn : Callable[..., Any]
            The function to be called for each received message. The function
            should accept a single argument containing the message payload.
        msgfrom : float, optional
            A timestamp in nanoseconds specifying the earliest message to
            deliver upon subscription. The default `rp.Now` starts from the
            moment of subscription, ignoring previously published messages.
        topic : str, optional
            The topic to subscribe to. If `None`, the callback function's name
            (`fn.__name__`) is used as the topic identifier.

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If the subscription request is rejected by the broker or fails due
            to an internal error.
        RembusTimeout
            If no response is received within the configured timeout.
        """
        if topic is None:
            topic = fn.__name__

        if self.isbroker():
            if isinstance(self._router, KeySpaceRouter):
                await self._router.subscribe_handler(self, topic)
        else:
            await self.setting(topic, rp.ADD_INTEREST, {"msg_from": msgfrom})

        self.router.handler[topic] = fn
        return self

    async def unsubscribe(self, fn: Callable[..., Any] | str):
        """
        Unsubscribe a callback function or topic name from receiving messages.

        This removes a previously registered subscription. If a function is given,
        it will be unsubscribed from the topic it was originally subscribed to.
        If a topic name (string) is provided, all callbacks subscribed to that topic
        will be removed.

        Parameters
        ----------
        fn : Callable[..., Any] or str
            The callback function to remove from its topic subscription, or the
            topic name to remove all subscriptions for.

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If the unsubscription fails due to a broker error or internal failure.
        RembusTimeout
            If the broker does not respond within the configured timeout.
        """
        if isinstance(fn, str):
            topic = fn
        else:
            topic = fn.__name__

        if self.isbroker():
            if isinstance(self._router, KeySpaceRouter):
                await self._router.unsubscribe_handler(self, topic)
        else:
            await self.setting(topic, rp.REMOVE_INTEREST)

        self.router.handler.pop(topic, None)
        return self

    async def expose(self, fn: Callable[..., Any], topic: Optional[str] = None):
        """
        Expose a function as a remote procedure call (RPC) handler.

        Once exposed, the function can be called remotely by other components
        through the Rembus messaging system. The `topic` identifies the RPC
        endpoint.
        If `topic` is not provided, the function's name (`fn.__name__`)
        is used as the topic name.

        Parameters
        ----------
        fn : Callable[..., Any]
            The function to expose as an RPC handler. The function should accept
            arguments corresponding to the RPC request parameters.
        topic : str, optional
            The RPC topic name under which the function is exposed. Defaults to
            the function's name if not specified.

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If exposing the function fails due to an internal error.
        RembusTimeout
            If the broker does not acknowledge the RPC exposure within the
            configured timeout.
        """
        if topic is None:
            topic = fn.__name__

        self.router.handler[topic] = fn
        await self.setting(topic, rp.ADD_IMPL)

    async def unexpose(
        self, fn: Callable[..., Any] | str, topic: Optional[str] = None
    ):
        """
        Remove a previously exposed function from being a remote procedure call (RPC) handler.

        After calling this method, the function will no longer be callable remotely
        through the Rembus messaging system. The topic identifies the RPC endpoint;
        if not provided, the function's name (`fn.__name__`) is used.

        Parameters
        ----------
        fn : Callable[..., Any] | str
            The function or function name to unexpose as an RPC handler.
        topic : str, optional
            The RPC topic name under which the function was previously exposed.
            Defaults to the function's name if not specified.

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If unexposing the function fails due to an internal error.
        RembusTimeout
            If the broker does not acknowledge the RPC removal within the configured timeout.
        """
        if isinstance(fn, str):
            topic = fn
        else:
            topic = fn.__name__

        self.router.handler.pop(topic, None)
        await self.setting(topic, rp.REMOVE_IMPL)

    async def close(self):
        """
        Close the connection to the Rembus broker and clean up associated
        resources.

        After calling this method, the component will no longer send or receive
        messages until it reconnects. All pending subscriptions and RPC handlers
        will remain registered on the broker, but the local connection state is
        reset.

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If an error occurs while closing the connection.
        """
        self.handler["phase"] = lambda: "CLOSED"
        await self.shutdown()
        await asyncio.sleep(0)  # let loop drain tasks

    async def wait(
        self, timeout: float | None = None, signal_handler: bool = True
    ):
        """
        Start the event loop to process incoming Rembus messages.

        This method runs the twin's main loop, receiving and dispatching
        messages to the appropriate subscriptions and RPC handlers. The loop
        continues until the connection is closed or an optional timeout expires.

        Parameters
        ----------
        timeout : float | None, optional
            Maximum time in seconds to wait before stopping the loop.
            If `None` (default), the loop runs indefinitely until manually
            stopped.
        signal_handler : bool, optional
            Whether to enable handling of OS signals (e.g., SIGINT) to
            gracefully
            terminate the loop. Defaults to `True`.

        Returns
        -------
        None

        Raises
        ------
        RembusError
            If an internal error occurs while processing messages.
        RembusTimeout
            If the loop times out while waiting for messages (only if `timeout`
            is set).
        """
        if not self.isbroker():
            await self.reactive()
        if self._supervisor_task is not None:
            try:
                if signal_handler:
                    self.register_shutdown()
                await asyncio.wait([self._supervisor_task], timeout=timeout)
            finally:
                await self.shutdown()


class ReplTwin(Twin):
    """
    A ReplTwin represents a virtual Rembus component that acts as a broker.

    Unlike a standard Twin, which connects a component to a Rembus broker,
    a ReplTwin simulates a broker itself.

    Attributes
    ----------
    Inherits all attributes and methods from :class:`Twin`.
    """

    def __init__(
        self,
        uid: RbURL,
        router: Supervised,
        isclient: bool = True,
        enc: int = rp.CBOR,
    ):
        super().__init__(uid, router, isclient, enc)

    def isbroker(self) -> bool:
        """Check if twin is a REPL"""
        return True

    def isopen(self) -> bool:
        """Check if the connection is open."""
        return any([t.isopen() for t in self.router.twins.values()])

    async def send(self, msg: rp.RembusMsg):
        """No-op send for REPL twin."""


class WsTwin(Twin):
    """
    A WsTwin represents a Rembus component communicating over WebSocket.

    This class wraps a :class:`Twin` with WebSocket transport.

    Attributes
    ----------
    Inherits all attributes and methods from :class:`Twin`.
    """

    def __init__(
        self,
        uid: RbURL,
        router: Supervised,
        isclient: bool = True,
        enc: int = rp.CBOR,
    ):
        super().__init__(uid, router, isclient, enc)

    def isbroker(self) -> bool:
        """
        Check whether this Twin represents a broker.

        Returns
        -------
        bool
            Always False for WsTwin, as it represents a connected component,
            not a broker.
        """
        return False

    def isopen(self) -> bool:
        """
        Check if the WebSocket connection is currently open.

        Returns
        -------
        bool
            True if the WebSocket connection is open; False if the socket
            is not connected or has not been initialized.
        """
        if self.socket is None:
            return False

        return self.socket.state == websockets.State.OPEN

    async def connect(self):
        """
        Establish a WebSocket connection to the Rembus broker.

        This method handles SSL if the URL scheme is `wss`, sets up the
        receiver task, and performs login if the Twin has a registered name.

        Raises
        ------
        RembusError
            If login fails after connecting to the broker.
        """
        broker_url = self.uid.netlink
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
                await self.login()
            except Exception as e:
                await self.close()
                raise rp.RembusError(rp.STS_ERROR, "login failed") from e

        self.handler["phase"] = lambda: "CONNECTED"

    async def send(self, msg: rp.RembusMsg):
        """
        Send a Rembus message over the WebSocket connection.

        Parameters
        ----------
        msg : rp.RembusMsg

        Notes
        -----
        Any exceptions during sending are logged as errors but not raised.
        """
        pkt = msg.to_payload(self.enc)
        await self._send(pkt)

    async def _close_socket(self):
        if self.socket:
            await self.socket.close()

        self.socket = None


class MqttTwin(Twin):
    """
    An MqttTwin represents a Rembus component communicating over MQTT.

    This class wraps a Twin with MQTT transport, allowing the component
    to act as a publisher or subscriber connected to a MQTT broker.

    Attributes
    ----------
    Inherits all attributes and methods from :class:`Twin`.
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

    def isbroker(self) -> bool:
        """Always False for the MqttTwin subclass."""
        return False

    async def on_message(self, client, topic, payload, qos, properties):
        """
        Handle incoming MQTT messages and route them to the router's inbox.

        This method is called automatically by the MQTT client whenever a
        message is received on a subscribed topic. It forwards the message
        payload to the appropriate router for processing.
        """
        logger.debug("[MQTT] (QOS=%s) %s: %s", qos, topic, payload)

        data = json.loads(payload)

        msg = rp.PubSubMsg(
            topic=topic,
            data=data if isinstance(data, list) else [data],
            flags=qos,
        )
        logger.debug("[%s] mqtt msg: %s", self, msg)
        await self._router.inbox.put(msg)

    async def connect(self):
        """
        Establish a connection to the configured MQTT broker.

        This method connects the MQTT client to the broker endpoint specified
        in the Twin configuration. After calling this, the Twin can publish
        and subscribe to topics over MQTT.
        """
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

    async def send(self, msg: rp.RembusMsg):
        """
        Send a Rembus message over MQTT.

        Only Pub/Sub messages (:class:`rp.PubSubMsg`) are supported
        on the MQTT transport. Attempting to send other message types
        will raise a :exc:`TypeError`.

        Parameters
        ----------
        msg : rp.RembusMsg
            The Rembus message to send. Must be an instance of
            :class:`rp.PubSubMsg`.

        Raises
        ------
        TypeError
            If `msg` is not an instance of :class:`rp.PubSubMsg`.
        """
        if isinstance(msg, rp.PubSubMsg):
            # If data has a single item, send just that item
            data_to_send = msg.data if len(msg.data) > 1 else msg.data[0]
            self.socket.publish(msg.topic, json.dumps(data_to_send))
        else:
            raise TypeError(
                f"MQTT transport cannot send messages of type {type(msg).__name__}:"
                "Only rp.PubSubMsg is supported."
            )

    def isopen(self) -> bool:
        """
        Check whether the MQTT connection is currently open.

        Returns
        -------
        bool
            True if the connection to the MQTT broker is active, False otherwise.
        """
        sts = False
        if self.socket is not None and self.socket.is_connected:
            sts = True
        return sts

    async def _close_socket(self):
        if self.socket:
            await self.socket.disconnect()

        self.socket = None
