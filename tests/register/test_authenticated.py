"""Test the authenticated connection mode of the router."""

import asyncio
import base64
import os
from types import SimpleNamespace
import cbor2
import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect
import rembus
import rembus.protocol as rp
import rembus.settings as rs
from tools.tenant import add_tenant

def echo(x):
    return x


NAME = "test_authenticated"
PIN = "11223344"
PORT = 8991


def test_authenticated_mode():
    """Only authenticated components can send commands."""
    add_tenant(".", PIN)
    try:
        os.remove(rs.key_file(rs.DEFAULT_BROKER, NAME))
    except FileNotFoundError:
        pass

    server = rembus.node(port=PORT, authenticated=True)

    # Registration is allowed without authentication.
    rembus.register(f"ws://:{PORT}/{NAME}", PIN)

    # The anonymous mode is not allowed.
    with pytest.raises(ValueError):
        rembus.node(f"ws://:{PORT}", authenticated=True)

    # An unregistered component cannot authenticate.
    with pytest.raises(rp.RembusError):
        rembus.node(f"ws://:{PORT}/unknown_cmp", authenticated=True)

    # The registered component is authenticated.
    cmp = rembus.node(f"ws://:{PORT}/{NAME}", authenticated=True)
    server.expose(echo)
    assert cmp.rpc("echo", 1) == 1
    cmp.unregister()
    cmp.close()
    server.close()


def test_anonymous_loopback_component_is_allowed():
    """Anonymous components connected from the loopback are allowed."""
    server = rembus.node(port=PORT, authenticated=True)
    try:
        server.expose(echo)
        cmp = rembus.node(f"ws://:{PORT}")
        try:
            assert cmp.rpc("echo", 1) == 1
        finally:
            cmp.close()
    finally:
        server.close()


def test_anonymous_remote_component_is_disconnected(monkeypatch):
    """A remote component that does not authenticate cannot send commands."""
    monkeypatch.setattr(
        rembus.router.Router, "_remote_host", lambda self, twin: "10.0.0.1"
    )
    server = rembus.node(port=PORT, authenticated=True)
    try:
        server.expose(echo)
        cmp = rembus.node(f"ws://:{PORT}")
        try:
            with pytest.raises((rp.RembusTimeout, rp.RembusConnectionClosed)):
                cmp.rpc("echo", 1)
        finally:
            cmp.close()
    finally:
        server.close()


def _recv_response(ws, timeout=5):
    msg = rp.cbor_parse(cbor2.loads(ws.recv(timeout=timeout)))
    assert isinstance(msg, rp.ResMsg)
    return msg


def test_identity_during_pending_challenge():
    """The Identity response is deferred until the attestation outcome."""
    name = "test_identity_pending"
    add_tenant(".", PIN)
    try:
        os.remove(rs.key_file(rs.DEFAULT_BROKER, name))
    except FileNotFoundError:
        pass

    server = rembus.node(port=PORT, authenticated=True)
    try:
        rembus.register(f"ws://:{PORT}/{name}", PIN)
        with connect(f"ws://127.0.0.1:{PORT}") as ws:
            challenge = _recv_response(ws)
            assert challenge.id == rp.CONNECTION_ID
            assert challenge.status == rp.STS_CHALLENGE

            identity_id = 0x1234
            ws.send(
                rp.IdentityMsg(id=identity_id, cid=name).to_payload(rp.CBOR)
            )
            # No response, and no new challenge, until the attestation.
            with pytest.raises(TimeoutError):
                ws.recv(timeout=0.5)

            data = (
                base64.b64decode(challenge.data)
                if isinstance(challenge.data, str)
                else bytes(challenge.data)
            )
            plain = cbor2.dumps([data, name])
            signature = rp.load_private_key(name).sign(
                plain, padding.PKCS1v15(), hashes.SHA256()
            )
            ws.send(
                rp.AttestationMsg(
                    id=rp.CONNECTION_ID, cid=name, signature=signature
                ).to_payload(rp.CBOR)
            )
            att = _recv_response(ws)
            assert att.id == rp.CONNECTION_ID
            assert att.status == rp.STS_OK
            ident = _recv_response(ws)
            assert ident.id == identity_id
            assert ident.status == rp.STS_OK
            assert ident.data is None
    finally:
        cmp = rembus.node(f"ws://:{PORT}/{name}", authenticated=True)
        cmp.unregister()
        cmp.close()
        server.close()


def test_failed_attestation_closes_connection():
    """A failed attestation is answered, then the connection is closed."""
    name = "test_failed_attestation"
    add_tenant(".", PIN)
    try:
        os.remove(rs.key_file(rs.DEFAULT_BROKER, name))
    except FileNotFoundError:
        pass

    server = rembus.node(port=PORT, authenticated=True)
    try:
        rembus.register(f"ws://:{PORT}/{name}", PIN)
        with connect(f"ws://127.0.0.1:{PORT}") as ws:
            challenge = _recv_response(ws)
            assert challenge.status == rp.STS_CHALLENGE

            ws.send(
                rp.AttestationMsg(
                    id=rp.CONNECTION_ID, cid=name, signature=b"invalid"
                ).to_payload(rp.CBOR)
            )
            att = _recv_response(ws)
            assert att.status == rp.STS_GENERIC_ERROR

            # The connection is still open right after the response ...
            with pytest.raises(TimeoutError):
                ws.recv(timeout=0.2)
            # ... and closed by the router after ~0.5 s.
            with pytest.raises(ConnectionClosed):
                ws.recv(timeout=2)
    finally:
        cmp = rembus.node(f"ws://:{PORT}/{name}", authenticated=True)
        cmp.unregister()
        cmp.close()
        server.close()


def test_identity_on_identified_connection_is_rejected():
    """Identity and Attestation on an identified connection get status 10."""
    server = rembus.node(port=PORT)
    try:
        with connect(f"ws://127.0.0.1:{PORT}") as ws:
            ws.send(rp.IdentityMsg(id=1, cid="first").to_payload(rp.CBOR))
            first = _recv_response(ws)
            assert first.status == rp.STS_OK

            ws.send(rp.IdentityMsg(id=2, cid="second").to_payload(rp.CBOR))
            second = _recv_response(ws)
            assert second.id == 2
            assert second.status == rp.STS_GENERIC_ERROR

            ws.send(
                rp.AttestationMsg(
                    id=3, cid="second", signature=b"x"
                ).to_payload(rp.CBOR)
            )
            att = _recv_response(ws)
            assert att.id == 3
            assert att.status == rp.STS_GENERIC_ERROR

            # the connection is still usable and keeps its identity
            ws.send(rp.IdentityMsg(id=4, cid="first").to_payload(rp.CBOR))
            again = _recv_response(ws)
            assert again.status == rp.STS_GENERIC_ERROR
    finally:
        server.close()


def test_reconnect_stops_on_authentication_failure():
    """An authentication failure is not retried."""
    calls = []

    async def connect_fail():
        calls.append(1)
        raise rp.RembusAuthError(rp.STS_GENERIC_ERROR, "login failed")

    async def setup():
        pass

    twin = SimpleNamespace(
        connect=connect_fail,
        setup=setup,
        handler={},
        reconnect_task="task",
    )
    asyncio.run(
        asyncio.wait_for(rembus.twin.Twin._reconnect(twin), timeout=5)
    )
    assert len(calls) == 1
    assert twin.handler["phase"]() == "CLOSED"
    assert twin.reconnect_task is None
