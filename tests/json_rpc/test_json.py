import base64
import orjson as json
import time
import pytest
import rembus


def my_service(x, y):
    return x + y


def test_jsonrpc_rpc(server):
    rb = rembus.node("mynode", enc=rembus.JSON)
    rb.expose(my_service)
    res = rb.rpc("rid")
    assert res == "broker"
    rb.close()


def test_jsonrpc_publish(server):
    rb = rembus.node("mynode", enc=rembus.JSON)
    rb.publish("my_topic")
    rb.publish("my_topic", qos=rembus.protocol.QOS1)
    rb.publish("my_topic", qos=rembus.protocol.QOS2)
    time.sleep(1)
    rb.close()


def test_jsonrpc_error(server):
    rb = rembus.node("mynode", enc=rembus.JSON)
    with pytest.raises(rembus.protocol.RembusError):
        rb.rpc("unknown_service")
    rb.close()


def test_jsonrpc_invalid_request():
    rb = rembus.node("mynode", enc=rembus.JSON)
    pkt = {"jsorpc": "2.0", "id": 1234, "params": {}}

    # Server-side throws an exception and close the connection.
    # The client detect the connection down and reconnect.
    rb._runner.run(rb._rb._send(json.dumps(pkt)))
    rb.close()


def test_jsonrpc_invalid_response():
    rb = rembus.node("mynode", enc=rembus.JSON)
    pkt = {
        "jsorpc": "2.0",
        "id": 1234,
        "result": {"type": 999},  # the type field has an invalid value
    }

    # Server-side throws an exception and close the connection.
    # The client detect the connection down and reconnect.
    rb._runner.run(rb._rb._send(json.dumps(pkt)))
    rb.close()


def test_jsonrpc_invalid_payload():
    rb = rembus.node("mynode", enc=rembus.JSON)
    pkt = {
        "jsorpc": "2.0",
        "id": 1234,
        # missing params, result or errors property
    }

    # Server-side throws an exception and close the connection.
    # The client detect the connection down and reconnect.
    rb._runner.run(rb._rb._send(json.dumps(pkt)))
    rb.close()


JSON_REKEY_CID = "test_json_rekey"
JSON_REKEY_PIN = "11223344"


def test_jsonrpc_rekey_decode():
    """A JSON-RPC Rekey request is decoded to a RekeyMsg (not Register)."""
    pkt = json.dumps(
        {
            "rembus": "1.0",
            "id": "123",
            "method": "mycid",
            "params": {
                "type": 7,
                "pin": "cGlu",
                "key_val": "a2V5",
                "key_type": 2,
            },
        }
    )
    msg = rembus.protocol.jsonrpc_parse(pkt)
    assert isinstance(msg, rembus.protocol.RekeyMsg)
    assert (msg.cid, msg.pin, msg.pubkey, msg.type) == (
        "mycid",
        b"pin",
        b"key",
        2,
    )
    wire = json.loads(msg.to_payload(rembus.JSON))
    assert wire["method"] == "mycid"
    assert wire["params"]["type"] == 7


def _read(fn):
    with open(fn, "rb") as f:
        return f.read()


def test_jsonrpc_rekey(server):
    """Rekey over JSON-RPC: failures keep the key, success replaces it."""
    import os
    import rembus.settings as rs

    cid, pin = JSON_REKEY_CID, JSON_REKEY_PIN
    # the module server is already running: set the tenant secret in memory
    prev_tenants = dict(server.router.tenants)
    server.router.tenants["."] = pin
    try:
        os.remove(rs.key_file(rs.DEFAULT_BROKER, cid))
    except FileNotFoundError:
        pass

    url = f"ws://:8338/{cid}"
    rembus.register(url, pin, enc=rembus.JSON)
    old_pub = _read(rs.key_file(rs.DEFAULT_BROKER, cid))
    old_secret = _read(os.path.join(rs.rembus_dir(), cid, ".secret"))

    rb = rembus.node(url, enc=rembus.JSON)

    # wrong pin => error with status 10, old key kept
    with pytest.raises(rembus.protocol.RembusError):
        rb.rekey("00000000")
    assert _read(rs.key_file(rs.DEFAULT_BROKER, cid)) == old_pub

    # raw invalid key: the error member carries status 10
    async def bad_rekey():
        twin = rb._rb
        fut = await twin._send_message(
            lambda id: rembus.protocol.RekeyMsg(
                id=id,
                cid=cid,
                pin=base64.b64encode(
                    rembus.protocol.pin_proof(pin, cid)
                ).decode(),
                pubkey=base64.b64encode(b"garbage").decode(),
                type=rembus.protocol.SIG_RSA,
            )
        )
        return await twin.wait_response(fut)

    res = rb.exec(bad_rekey)
    assert res.status == rembus.protocol.STS_GENERIC_ERROR
    assert _read(rs.key_file(rs.DEFAULT_BROKER, cid)) == old_pub

    # success
    rb.rekey(pin)
    assert rb.rpc("rid") == "broker"
    rb.close()
    assert _read(rs.key_file(rs.DEFAULT_BROKER, cid)) != old_pub

    rb = rembus.node(url, enc=rembus.JSON)
    assert rb.rpc("rid") == "broker"
    rb.close()

    # the old private key no longer authenticates
    secret_fn = os.path.join(rs.rembus_dir(), cid, ".secret")
    new_secret = _read(secret_fn)
    with open(secret_fn, "wb") as f:
        f.write(old_secret)
    with pytest.raises(rembus.protocol.RembusError):
        rembus.node(url, enc=rembus.JSON)
    with open(secret_fn, "wb") as f:
        f.write(new_secret)

    rb = rembus.node(url, enc=rembus.JSON)
    rb.unregister()
    rb.close()
    server.router.tenants.clear()
    server.router.tenants.update(prev_tenants)


def _without_key_type(msg_cls):
    """A message class whose JSON payload omits `key_type`."""

    class NoKeyType(msg_cls):
        def to_payload(self, enc):
            pkt = json.loads(super().to_payload(enc))
            del pkt["params"]["key_type"]
            return json.dumps(pkt).decode()

    return NoKeyType


def test_jsonrpc_key_type_default_decode():
    """An absent key_type defaults to RSA for Register and Rekey."""
    for ttype, cls in (
        (10, rembus.protocol.RegisterMsg),
        (7, rembus.protocol.RekeyMsg),
    ):
        pkt = json.dumps(
            {
                "rembus": "1.0",
                "id": "123",
                "method": "mycid",
                "params": {"type": ttype, "pin": "cGlu", "key_val": "a2V5"},
            }
        )
        msg = rembus.protocol.jsonrpc_parse(pkt)
        assert isinstance(msg, cls)
        assert msg.type == rembus.protocol.SIG_RSA


def test_jsonrpc_register_rekey_default_key_type(server):
    """Absent key_type: RSA key accepted, ECDSA key rejected with status 10."""
    import os
    import rembus.protocol as rp
    import rembus.settings as rs

    cid, pin = "test_json_keytype", "11223344"
    prev_tenants = dict(server.router.tenants)
    server.router.tenants["."] = pin
    try:
        os.remove(rs.key_file(rs.DEFAULT_BROKER, cid))
    except FileNotFoundError:
        pass

    def send(rb, cls, pubkey):
        async def go():
            twin = rb._rb
            fut = await twin._send_message(
                lambda id: _without_key_type(cls)(
                    id=id,
                    cid=cid,
                    pin=base64.b64encode(rp.pin_proof(pin, cid)).decode(),
                    pubkey=base64.b64encode(pubkey).decode(),
                    type=rp.SIG_RSA,
                )
            )
            return await twin.wait_response(fut)

        return rb.exec(go)

    rsa_pub = rp.pem_public_key(rp.rsa_private_key())
    ec_pub = rp.pem_public_key(rp.ecdsa_private_key())

    anon = rembus.node(f"ws://:8338/{cid}", enc=rembus.JSON)
    assert send(anon, rp.RegisterMsg, ec_pub).status == rp.STS_GENERIC_ERROR
    assert not rp.isregistered(rs.DEFAULT_BROKER, cid)
    assert send(anon, rp.RegisterMsg, rsa_pub).status == rp.STS_OK
    anon.close()

    # Rekey needs an authenticated session: replace the registered key with
    # one whose private half is stored locally
    priv = rp.rsa_private_key()
    rp.save_private_key(cid, priv)
    with open(rs.key_file(rs.DEFAULT_BROKER, cid), "wb") as f:
        f.write(rp.pem_public_key(priv))

    rb = rembus.node(f"ws://:8338/{cid}", enc=rembus.JSON)
    old = open(rs.key_file(rs.DEFAULT_BROKER, cid), "rb").read()
    assert send(rb, rp.RekeyMsg, ec_pub).status == rp.STS_GENERIC_ERROR
    assert open(rs.key_file(rs.DEFAULT_BROKER, cid), "rb").read() == old
    assert send(rb, rp.RekeyMsg, rsa_pub).status == rp.STS_OK
    assert open(rs.key_file(rs.DEFAULT_BROKER, cid), "rb").read() == rsa_pub
    rb.close()

    os.remove(rs.key_file(rs.DEFAULT_BROKER, cid))
    server.router.tenants.clear()
    server.router.tenants.update(prev_tenants)
