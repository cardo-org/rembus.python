"""Test the two-step, nonce-bound Register and Rekey."""

import base64
import hashlib
import os
import cbor2
import pytest
from websockets.sync.client import connect
import rembus
import rembus.protocol as rp
import rembus.router as rr
import rembus.settings as rs
from tools.tenant import add_tenant

PIN = "11223344"
PORT = 8992
NAME = "test_nonce"


def _fresh(name):
    try:
        os.remove(rs.key_file(rs.DEFAULT_BROKER, name))
    except FileNotFoundError:
        pass


def _send(ws, cls, cid, pin, pubkey=b"", keytype=0):
    msg_id = int.from_bytes(os.urandom(8), "big")
    msg = cls(id=msg_id, cid=cid, pin=pin, pubkey=pubkey, type=keytype)
    ws.send(msg.to_payload(rp.CBOR))
    res = rp.cbor_parse(cbor2.loads(ws.recv(timeout=5)))
    assert isinstance(res, rp.ResMsg)
    assert res.id == msg_id
    return res


def _challenge(ws, cid, cls=rp.RegisterMsg):
    res = _send(ws, cls, cid, b"")
    assert res.status == rp.STS_CHALLENGE
    assert len(res.data) >= 16
    return bytes(res.data)


def _proof_req(ws, cid, nonce, pubkey, keytype=rp.SIG_RSA, cls=rp.RegisterMsg):
    proof = rp.pin_proof(PIN, cid, nonce, pubkey, keytype)
    return _send(ws, cls, cid, proof, pubkey, keytype)


@pytest.fixture(name="server")
def server_fixture():
    add_tenant(".", PIN)
    srv = rembus.node(port=PORT)
    yield srv
    srv.close()


@pytest.fixture(name="privkey")
def privkey_fixture():
    return rp.rsa_private_key()


@pytest.fixture(name="pubkey")
def pubkey_fixture(privkey):
    return rp.pem_public_key(privkey)


def test_two_step_register(server):
    """The client API registers through the challenge/proof exchange."""
    _fresh(NAME)
    rembus.register(f"ws://:{PORT}/{NAME}", PIN)
    assert rp.isregistered(rs.DEFAULT_BROKER, NAME)
    rb = rembus.node(f"ws://:{PORT}/{NAME}")
    rb.rekey(PIN)
    assert rb.rpc("rid") == rs.DEFAULT_BROKER
    rb.unregister()
    rb.close()


def test_challenge_hides_registration(server, pubkey):
    """A challenge is issued for registered and unknown cids alike."""
    _fresh(NAME)
    rembus.register(f"ws://:{PORT}/{NAME}", PIN)
    with connect(f"ws://127.0.0.1:{PORT}") as ws:
        assert len(_challenge(ws, NAME)) == 32
        assert len(_challenge(ws, "test_nonce_unknown")) == 32
        # the registered cid is reported only at the proof step
        nonce = _challenge(ws, NAME)
        res = _proof_req(ws, NAME, nonce, pubkey)
        assert res.status == rp.STS_NAME_ALREADY_TAKEN
    rembus.node(f"ws://:{PORT}/{NAME}").unregister()


def test_register_proof_failures(server, pubkey):
    cid = "test_nonce_fail"
    _fresh(cid)
    with connect(f"ws://127.0.0.1:{PORT}") as ws:
        # proof with no nonce
        res = _proof_req(ws, cid, os.urandom(32), pubkey)
        assert res.status == rp.STS_GENERIC_ERROR

        # legacy one-step proof
        legacy = hashlib.sha256(cbor2.dumps([PIN, cid])).digest()
        res = _send(ws, rp.RegisterMsg, cid, legacy, pubkey, rp.SIG_RSA)
        assert res.status == rp.STS_GENERIC_ERROR

        # legacy proof after a challenge fails and consumes the nonce
        nonce = _challenge(ws, cid)
        res = _send(ws, rp.RegisterMsg, cid, legacy, pubkey, rp.SIG_RSA)
        assert res.status == rp.STS_GENERIC_ERROR
        res = _proof_req(ws, cid, nonce, pubkey)
        assert res.status == rp.STS_GENERIC_ERROR

        # pin length other than 0 or 32
        res = _send(ws, rp.RegisterMsg, cid, b"x" * 16, pubkey, rp.SIG_RSA)
        assert res.status == rp.STS_GENERIC_ERROR

        # proof bound to another key
        nonce = _challenge(ws, cid)
        proof = rp.pin_proof(PIN, cid, nonce, os.urandom(8), rp.SIG_RSA)
        res = _send(ws, rp.RegisterMsg, cid, proof, pubkey, rp.SIG_RSA)
        assert res.status == rp.STS_GENERIC_ERROR
    assert not rp.isregistered(rs.DEFAULT_BROKER, cid)


def test_replay_and_reuse(server, privkey, pubkey):
    """A proof (or nonce) is single-use, even across a new connection."""
    cid = "test_nonce_replay"
    _fresh(cid)
    with connect(f"ws://127.0.0.1:{PORT}") as ws:
        nonce = _challenge(ws, cid)
        assert _proof_req(ws, cid, nonce, pubkey).status == rp.STS_OK
        rp.save_private_key(cid, privkey)
        # replay of the captured proof on the same connection
        res = _proof_req(ws, cid, nonce, pubkey)
        assert res.status == rp.STS_GENERIC_ERROR
    rembus.node(f"ws://:{PORT}/{cid}").unregister()

    with connect(f"ws://127.0.0.1:{PORT}") as ws:
        # replay on another connection
        res = _proof_req(ws, cid, nonce, pubkey)
        assert res.status == rp.STS_GENERIC_ERROR

        # failed proof consumes the nonce: the valid proof is then rejected
        nonce = _challenge(ws, cid)
        res = _proof_req(ws, cid, os.urandom(32), pubkey)
        assert res.status == rp.STS_GENERIC_ERROR
        res = _proof_req(ws, cid, nonce, pubkey)
        assert res.status == rp.STS_GENERIC_ERROR

        # a new challenge invalidates the previous nonce
        first = _challenge(ws, cid)
        second = _challenge(ws, cid)
        assert first != second
        assert _proof_req(ws, cid, first, pubkey).status == (
            rp.STS_GENERIC_ERROR
        )
    assert not rp.isregistered(rs.DEFAULT_BROKER, cid)


def test_nonce_binding(server, privkey, pubkey):
    """The nonce is bound to connection, cid and operation."""
    cid = "test_nonce_bind"
    _fresh(cid)
    with connect(f"ws://127.0.0.1:{PORT}") as ws1:
        with connect(f"ws://127.0.0.1:{PORT}") as ws2:
            nonce = _challenge(ws1, cid)
            res = _proof_req(ws2, cid, nonce, pubkey)
            assert res.status == rp.STS_GENERIC_ERROR

        # other cid
        nonce = _challenge(ws1, cid)
        res = _proof_req(ws1, "test_nonce_other", nonce, pubkey)
        assert res.status == rp.STS_GENERIC_ERROR

        # other operation (Rekey proof for a Register nonce)
        nonce = _challenge(ws1, cid)
        res = _proof_req(ws1, cid, nonce, pubkey, cls=rp.RekeyMsg)
        assert res.status == rp.STS_GENERIC_ERROR

        # the bound nonce is still valid
        nonce = _challenge(ws1, cid)
        assert _proof_req(ws1, cid, nonce, pubkey).status == rp.STS_OK
        rp.save_private_key(cid, privkey)
    rembus.node(f"ws://:{PORT}/{cid}").unregister()


def test_expired_nonce(server, pubkey, monkeypatch):
    cid = "test_nonce_expired"
    _fresh(cid)
    monkeypatch.setattr(rr, "PIN_NONCE_TTL", -1.0)
    with connect(f"ws://127.0.0.1:{PORT}") as ws:
        nonce = _challenge(ws, cid)
        res = _proof_req(ws, cid, nonce, pubkey)
        assert res.status == rp.STS_GENERIC_ERROR
    assert not rp.isregistered(rs.DEFAULT_BROKER, cid)


def test_outstanding_nonces_bounded(server):
    _fresh(NAME)
    with connect(f"ws://127.0.0.1:{PORT}") as ws:
        for i in range(rr.MAX_PIN_NONCES):
            _challenge(ws, f"test_nonce_many{i}")
        res = _send(ws, rp.RegisterMsg, "test_nonce_extra", b"")
        assert res.status == rp.STS_GENERIC_ERROR
        # reissuing for an already challenged cid is not blocked
        _challenge(ws, "test_nonce_many0")


def test_rekey_nonce(server):
    """Rekey challenge needs authentication; proof is nonce-bound."""
    cid = "test_nonce_rekey"
    _fresh(cid)
    rembus.register(f"ws://:{PORT}/{cid}", PIN)
    newkey = rp.rsa_private_key()
    pub = rp.pem_public_key(newkey)
    rb = rembus.node(f"ws://:{PORT}/{cid}")

    def raw(pin, cls=rp.RekeyMsg, pubkey=b"", keytype=0, nonce=None):
        if nonce is not None:
            pin = rp.pin_proof(PIN, cid, nonce, pubkey, keytype)

        async def go():
            twin = rb._rb
            fut = await twin._send_message(
                lambda id: cls(
                    id=id, cid=cid, pin=pin, pubkey=pubkey, type=keytype
                )
            )
            return await twin.wait_response(fut)

        return rb.exec(go)

    # legacy proof and proof without nonce
    legacy = hashlib.sha256(cbor2.dumps([PIN, cid])).digest()
    assert raw(legacy, pubkey=pub, keytype=1).status == rp.STS_GENERIC_ERROR
    res = raw(b"", nonce=os.urandom(32), pubkey=pub, keytype=1)
    assert res.status == rp.STS_GENERIC_ERROR

    # Register nonce does not satisfy a Rekey proof
    nonce = raw(b"", cls=rp.RegisterMsg).data
    res = raw(b"", nonce=nonce, pubkey=pub, keytype=1)
    assert res.status == rp.STS_GENERIC_ERROR

    # replay of a captured Rekey proof
    nonce = raw(b"").data
    assert raw(b"", nonce=nonce, pubkey=pub, keytype=1).status == rp.STS_OK
    rp.save_private_key(cid, newkey)
    res = raw(b"", nonce=nonce, pubkey=pub, keytype=1)
    assert res.status == rp.STS_GENERIC_ERROR
    rb.close()

    # Rekey challenge by an unauthenticated connection is refused
    with connect(f"ws://127.0.0.1:{PORT}") as ws:
        res = _send(ws, rp.RekeyMsg, cid, b"")
        assert res.status == rp.STS_GENERIC_ERROR
    rembus.node(f"ws://:{PORT}/{cid}").unregister()


def test_json_two_step(server):
    """JSON: empty pin is the challenge request, the nonce is base64."""
    cid = "test_nonce_json"
    _fresh(cid)
    rembus.register(f"ws://:{PORT}/{cid}", PIN, enc=rembus.JSON)
    rb = rembus.node(f"ws://:{PORT}/{cid}", enc=rembus.JSON)
    rb.rekey(PIN)

    async def challenge():
        twin = rb._rb
        fut = await twin._send_message(
            lambda id: rp.RekeyMsg(id=id, cid=cid, pin="", pubkey="", type=0)
        )
        return await twin.wait_response(fut)

    res = rb.exec(challenge)
    assert res.status == rp.STS_CHALLENGE
    assert len(base64.b64decode(res.data)) == 32
    rb.unregister()
    rb.close()
