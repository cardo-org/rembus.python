"""
Utility functions, messages and protocol constants related
to the Rembus protocol.
"""
import base64
import os
import logging
from enum import IntEnum
from typing import Any, List
import json
import cbor2
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa, ec
from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
from cryptography.hazmat.backends import default_backend
from narwhals.typing import IntoFrame
try:
    import polars as pl
    _HAS_POLARS = True
except ImportError:
    _HAS_POLARS = False
try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:
    _HAS_PANDAS = False
import pyarrow as pa
from pydantic import BaseModel, PrivateAttr
import rembus.settings as rs

logger = logging.getLogger(__name__)

WS_FRAME_MAXSIZE = 60 * 1024 * 1024

SIG_RSA = 0x1
SIG_ECDSA = 0x2

CBOR = 0
JSON = 1


class QOSLevel(IntEnum):
    """The Pub/Sub message QOS level."""
    QOS0 = 0x00
    QOS1 = 0x10
    QOS2 = 0x30


TYPE_IDENTITY = 0
TYPE_PUB = 1
TYPE_RPC = 2
TYPE_ADMIN = 3
TYPE_RESPONSE = 4
TYPE_ACK = 5
TYPE_ACK2 = 6
TYPE_UNREGISTER = 9
TYPE_REGISTER = 10
TYPE_ATTESTATION = 11

STS_OK = 0
STS_ERROR = 0x0A
STS_CHALLENGE = 0x0B            # 11
STS_IDENTIFICATION_ERROR = 0X14  # 20
STS_METHOD_EXCEPTION = 0X28     # 40
STS_METHOD_ARGS_ERROR = 0X29    # 41
STS_METHOD_NOT_FOUND = 0X2A     # 42
STS_METHOD_UNAVAILABLE = 0X2B   # 43
STS_METHOD_LOOPBACK = 0X2C      # 44
STS_TARGET_NOT_FOUND = 0X2D     # 45
STS_TARGET_DOWN = 0X2E          # 46
STS_UNKNOWN_ADMIN_CMD = 0X2F    # 47
STS_NAME_ALREADY_TAKEN = 0X3C  # 60

DATAFRAME_TAG = 80

retcode = {
    STS_OK: 'OK',
    STS_ERROR: 'internal error',
    STS_IDENTIFICATION_ERROR: 'IDENTIFICATION_ERROR',
    STS_METHOD_EXCEPTION: 'METHOD_EXCEPTION',
    STS_METHOD_ARGS_ERROR: 'METHOD_ARGS_ERROR',
    STS_METHOD_NOT_FOUND: 'METHOD_NOT_FOUND',
    STS_METHOD_UNAVAILABLE: 'METHOD_UNAVAILABLE',
    STS_METHOD_LOOPBACK: 'METHOD_LOOPBACK',
    STS_TARGET_NOT_FOUND: 'TARGET_NOT_FOUND',
    STS_TARGET_DOWN: 'TARGET_DOWN',
    STS_UNKNOWN_ADMIN_CMD: 'UNKNOWN_ADMIN_CMD',
    STS_NAME_ALREADY_TAKEN: 'NAME_ALREADY_TAKEN',
}

BROKER_CONFIG = '__config__'
COMMAND = 'cmd'
ADD_INTEREST = 'subscribe'
REMOVE_INTEREST = 'unsubscribe'
ADD_IMPL = 'expose'
REMOVE_IMPL = 'unexpose'


def msgid():
    """Return an array of 16 random bytes."""
    return int.from_bytes(os.urandom(16))


def bytes2id(byte_data: bytearray) -> int:
    """
    Converts a 16-byte bytearray to a UInt128 integer.

    Args:
        byte_data: A bytearray of 16 bytes.

    Returns:
        A Python integer representing the UInt128 value.
    """
    if len(byte_data) != 16:
        raise ValueError("bytearray must be exactly 16 bytes long")

    # Convert bytes to integer (assuming little-endian, adjust if big-endian)
    return int.from_bytes(byte_data, byteorder='little', signed=False)


class RembusException(Exception):
    """Base class for all Rembus exceptions."""


class RembusTimeout(RembusException):
    """Raised when a Rembus message that expects a response times out."""

    def __str__(self):
        return 'request timeout'


class RembusConnectionClosed(RembusException):
    """Raised when a Rembus connection is closed unexpectedly."""

    def __str__(self):
        return 'connection down'


class RembusError(RembusException):
    """Raised when a Rembus message returns a generic error."""

    def __init__(self, status_code: int, msg: str | None = None):
        self.status = status_code
        self.message = msg

    def __str__(self):
        if self.message:
            return f'{retcode[self.status]}:{self.message}'
        else:
            return f'{retcode[self.status]}'


def to_bytes(val: int) -> bytes:
    """Convert an int to 16 bytes"""
    return val.to_bytes(16)


class RembusMsg(BaseModel):
    """Rembus message"""
    _twin: Any = PrivateAttr(default=None)

    @property
    def twin(self):
        """Return the Twin that owns the message"""
        return self._twin

    @twin.setter
    def twin(self, rb):
        self._twin = rb

    def to_payload(self, enc: int) -> bytes | str:
        """Return the message list of values to encode"""
        raise RuntimeError("abstract rembus message")


class RpcReqMsg(RembusMsg):
    """RPC request packet."""
    id: int
    topic: str
    data: Any = None
    target: str | None = None

    def to_payload(self, enc: int) -> bytes | str:
        """Return the RpcReqMsg list of values to encode"""
        if enc == CBOR:
            return cbor2.dumps(
                [TYPE_RPC, to_bytes(self.id), self.topic,
                 self.target, self.data]
            )

        return json.dumps({
            "jsonrpc": "2.0",
            "id": self.id,
            "method": self.topic,
            "params": self.data
        })


class ResMsg(RembusMsg):
    """Response packet."""
    id: int
    status: int
    data: Any = None
    _reqdata: Any = PrivateAttr()

    def to_payload(self, enc: int) -> bytes | str:
        """Return the ResMsg list of values to encode"""
        if enc == CBOR:
            return cbor2.dumps([
                TYPE_RESPONSE,
                to_bytes(self.id),
                self.status,
                df2tag(self.data)
            ])

        if self.status == STS_OK or self.status == STS_CHALLENGE:
            restype = "result"
        else:
            restype = "error"

        return json.dumps({
            "jsonrpc": "2.0",
            "id": self.id,
            restype: {
                "type": TYPE_RESPONSE,
                "sts": self.status,
                "data": self.data
            }
        })


class PubSubMsg(RembusMsg):
    """Pub/Sub message packet."""
    id: int | None = None
    topic: str
    data: Any = None
    flags: QOSLevel = QOSLevel.QOS0

    def to_payload(self, enc: int) -> bytes | str:
        """Return the PubSubMsg list of values to encode"""
        if self.flags == QOSLevel.QOS0 or self.id is None:
            if enc == CBOR:
                return cbor2.dumps([TYPE_PUB, self.topic, self.data])

            return json.dumps({
                "jsonrpc": "2.0",
                "method": self.topic,
                "params": self.data
            })
        else:
            if enc == CBOR:
                return cbor2.dumps([
                    TYPE_PUB | self.flags,
                    to_bytes(self.id),
                    self.topic,
                    self.data
                ])

            return json.dumps({
                "jsonrpc": "2.0",
                "id": self.id,
                "method": self.topic,
                "params": {
                    "type": self.flags,
                    "data": self.data
                }
            })


class AckMsg(RembusMsg):
    """Pub/Sub Ack message for QOS1 and QOS2"""
    id: int

    def to_payload(self, enc: int) -> bytes | str:
        """Return the AckMsg list of values to encode"""
        if enc == CBOR:
            return cbor2.dumps([TYPE_ACK, to_bytes(self.id)])

        return json.dumps({
            "jsonrpc": "2.0",
            "id": self.id,
            "result": {
                "type": TYPE_ACK
            }
        })


class Ack2Msg(RembusMsg):
    """Pub/Sub Ack2 message for QOS2"""
    id: int

    def to_payload(self, enc: int) -> bytes | str:
        """Return the Ack2Msg list of values to encode"""
        if enc == CBOR:
            return cbor2.dumps([TYPE_ACK2, to_bytes(self.id)])

        return json.dumps({
            "jsonrpc": "2.0",
            "id": self.id,
            "result": {
                "type": TYPE_ACK2
            }
        })


class AdminMsg(RembusMsg):
    """AdminMsg packet."""
    id: int
    topic: str
    data: Any = None

    def __str__(self):
        return f'AdminMsg:{self.topic}: {self.data}'

    def to_payload(self, enc: int) -> bytes | str:
        """Return the AdminMsg list of values to encode"""
        if enc == CBOR:
            return cbor2.dumps(
                [TYPE_ADMIN, to_bytes(self.id), self.topic, self.data]
            )

        return json.dumps({
            "jsonrpc": "2.0",
            "id": self.id,
            "method": self.topic,
            "params": {
                "type": TYPE_ADMIN,
                "data": self.data
            }
        })


class IdentityMsg(RembusMsg):
    """IdentityMsg packet.
    This message is sent by the component to identify itself
    to the remote peer.
    """
    id: int
    cid: str

    def __str__(self):
        return f'IdentityMsg:{self.cid}'

    def to_payload(self, enc: int) -> bytes | str:
        """Return the IdentityMsg payload"""
        if enc == CBOR:
            return cbor2.dumps([TYPE_IDENTITY, to_bytes(self.id), self.cid])

        return json.dumps({
            "jsonrpc": "2.0",
            "id": self.id,
            "method": "__identity__",
            "params": {
                "type": TYPE_IDENTITY,
                "cid": self.cid
            }
        })


class AttestationMsg(RembusMsg):
    """AttestationMsg packet.
    This message is sent by the component to authenticate
    its identity to the remote peer.
    """
    id: int
    cid: str
    signature: bytes | str

    def __str__(self):
        return f'AttestationMsg:{self.cid}'

    def to_payload(self, enc: int) -> bytes | str:
        """Return the AttestationMsg list of values to encode"""
        if enc == CBOR:
            return cbor2.dumps(
                [TYPE_ATTESTATION, to_bytes(self.id), self.cid, self.signature]
            )

        return json.dumps({
            "jsonrpc": "2.0",
            "id": self.id,
            "method": self.cid,
            "params": {
                "type": TYPE_ATTESTATION,
                "signature": self.signature
            }
        })


class RegisterMsg(RembusMsg):
    """RegisterMsg packet.
    This message is sent by the component to register
    its public key to the remote peer.
    """
    id: int
    cid: str
    pin: str
    pubkey: bytes | str
    type: int

    def __str__(self):
        return f'RegisterMsg:{self.cid}'

    def to_payload(self, enc: int) -> bytes | str:
        """Return the RegisterMsg list of values to encode"""
        if enc == CBOR:
            return cbor2.dumps([
                TYPE_REGISTER,
                to_bytes(self.id),
                self.cid,
                self.pin,
                self.pubkey,
                self.type
            ])

        return json.dumps({
            "jsonrpc": "2.0",
            "id": self.id,
            "method": self.cid,
            "params": {
                "type": TYPE_REGISTER,
                "pin": self.pin,
                "key_val": self.pubkey,
                "key_type": self.type
            }
        })


class UnregisterMsg(RembusMsg):
    """UnregisterMsg packet.
    This message is sent by the component to unregister
    its public key from the remote peer.
    """
    id: int

    def __str__(self):
        return f'UnregisterMsg:{self._twin}'

    def to_payload(self, enc: int) -> bytes | str:
        """Return the UnregisterMsg list of values to encode"""
        if enc == CBOR:
            return cbor2.dumps([TYPE_UNREGISTER, to_bytes(self.id)])

        return json.dumps({
            "jsonrpc": "2.0",
            "id": self.id,
            "method": "__unregister__",
            "params": {
                "type": TYPE_UNREGISTER
            }
        })


def isregistered(router_id, rid: str):
    """Check if a component identified by `rid` is registered in the router."""
    try:
        rs.key_file(router_id, rid)
        return True
    except FileNotFoundError:
        return False


def tohex(bs: bytes):
    """Return a string with bytes as hex numbers with 0xNN format."""
    return ' '.join(f'0x{x:02x}' for x in bs)


def jsonprc_request(pkt, msg_id, params) -> RembusMsg:
    """Parse a JSON_RPC request"""
    if isinstance(params, list):
        # Default to RPC request
        return RpcReqMsg(id=msg_id, topic=pkt["method"], data=params)
    else:
        msg_type = params.get("type")
        if msg_type in [QOSLevel.QOS1, QOSLevel.QOS2]:
            return PubSubMsg(
                id=msg_id,
                topic=pkt["method"],
                data=pkt.get("data"),
                flags=msg_type
            )
        elif msg_type == TYPE_IDENTITY:
            return IdentityMsg(id=msg_id, cid=params["cid"])
        elif msg_type == TYPE_ADMIN:
            return AdminMsg(
                id=msg_id,
                topic=pkt["method"],
                data=pkt.get("data")
            )
        elif msg_type == TYPE_ATTESTATION:
            sig = params.get("signature")
            return AttestationMsg(id=msg_id, cid=pkt["method"], signature=sig)
        elif msg_type == TYPE_REGISTER:
            pubkey = params.get("key_val")
            if isinstance(pubkey, str):
                pubkey = base64.b64decode(pubkey)

            return RegisterMsg(
                id=msg_id,
                cid=pkt["method"],
                pin=params.get("pin"),
                pubkey=pubkey,
                type=params.get("key_type")
            )
        elif msg_type == TYPE_UNREGISTER:
            return UnregisterMsg(
                id=msg_id
            )

        raise ValueError(f"{pkt}:invalid JSON-RPC request")


def jsonprc_response(pkt, msg_id, result) -> RembusMsg:
    """Parse a JSON_RPC success response"""

    msg_type = result.get("type")
    if msg_type == TYPE_RESPONSE or msg_type is None:
        status = result.get("sts", STS_OK)
        return ResMsg(id=msg_id, status=status, data=result.get("data"))
    elif msg_type == TYPE_ACK:
        return AckMsg(id=msg_id)
    elif msg_type == TYPE_ACK2:
        return Ack2Msg(id=msg_id)

    raise ValueError(f"{pkt}:invalid JSON-RPC response")


def jsonrpc_parse(payload) -> RembusMsg:
    """Get a Rembus message from a JSON-RPC string payload."""
    pkt = json.loads(payload)
    msg_id = pkt.get("id")
    if msg_id:
        # request-response message
        params = pkt.get("params")
        if params is not None:
            return jsonprc_request(pkt, msg_id, params)

        result = pkt.get("result")
        if result:
            return jsonprc_response(pkt, msg_id, result)

        err = pkt.get("error")
        if err:
            return jsonprc_response(pkt, msg_id, err)

    else:
        return PubSubMsg(topic=pkt["method"], data=pkt.get("data"))

    raise ValueError(f"{pkt}:JSON-RPC invalid payload")


def cbor_parse(pkt) -> RembusMsg:
    """Get a Rembus message from a CBOR packet."""
    type_byte = pkt[0]
    mtype = type_byte & 0x0F
    flags = type_byte & 0xF0
    if mtype == TYPE_PUB:
        if flags == QOSLevel.QOS0:
            return PubSubMsg(topic=pkt[1], data=pkt[2])
        else:
            return PubSubMsg(
                id=int.from_bytes(pkt[1]),
                topic=pkt[2],
                data=pkt[3],
                flags=flags
            )
    elif mtype == TYPE_RPC:
        return RpcReqMsg(
            id=int.from_bytes(pkt[1]),
            topic=pkt[2],
            target=pkt[3],
            data=pkt[4]
        )
    elif mtype == TYPE_RESPONSE:
        if len(pkt) > 3:
            data = pkt[3]
        else:
            data = None

        return ResMsg(
            id=int.from_bytes(pkt[1]), status=pkt[2], data=data
        )
    elif mtype == TYPE_ACK:
        return AckMsg(id=int.from_bytes(pkt[1]))
    elif mtype == TYPE_ACK2:
        return Ack2Msg(id=int.from_bytes(pkt[1]))
    elif mtype == TYPE_ADMIN:
        return AdminMsg(
            id=int.from_bytes(pkt[1]), topic=pkt[2], data=pkt[3]
        )
    elif mtype == TYPE_IDENTITY:
        return IdentityMsg(id=int.from_bytes(pkt[1]), cid=pkt[2])
    elif mtype == TYPE_ATTESTATION:
        return AttestationMsg(
            id=int.from_bytes(pkt[1]), cid=pkt[2], signature=pkt[3]
        )
    elif mtype == TYPE_REGISTER:
        return RegisterMsg(
            id=int.from_bytes(pkt[1]),
            cid=pkt[2],
            pin=pkt[3],
            pubkey=pkt[4],
            type=pkt[5]
        )
    elif mtype == TYPE_UNREGISTER:
        return UnregisterMsg(id=int.from_bytes(pkt[1]))

    raise ValueError('unknown message type')


def decode_dataframe(data: bytes) -> IntoFrame:
    """Decode a CBOR tagged value `data` to a pandas dataframe."""
    writer = pa.BufferOutputStream()
    writer.write(data)
    buf: pa.Buffer = writer.getvalue()
    with pa.ipc.open_stream(buf) as reader:
        table = reader.read_all()

    if _HAS_POLARS:
        return pl.from_arrow(table)
    elif _HAS_PANDAS:
        return table.to_pandas()
    else:
        raise ImportError("neither polars nor pandas is installed")


def encode_dataframe(df: IntoFrame) -> cbor2.CBORTag:
    """Encode a pandas dataframe `df` to a CBOR tag value."""
    if _HAS_POLARS and isinstance(df, pl.DataFrame):
        table = df.to_arrow()
    elif _HAS_PANDAS and isinstance(df, pd.DataFrame):
        table = pa.Table.from_pandas(df)
    else:
        raise TypeError(
            "unsupported dataframe type. Expected pandas or polars DataFrame"
        )

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write(table)
    buf = sink.getvalue()
    stream = pa.input_stream(buf)
    return cbor2.CBORTag(DATAFRAME_TAG, stream.read())


def encode(msg: list[Any]) -> bytes:
    """Encode message `msg`."""
    return cbor2.dumps(msg)


def tag2df(data: Any) -> Any:
    """Loop over `data` items and decode tagged values to dataframes."""
    if isinstance(data, list):
        for idx, val in enumerate(data):
            if isinstance(val, cbor2.CBORTag) and val.tag == DATAFRAME_TAG:
                data[idx] = decode_dataframe(val.value)
    elif isinstance(data, cbor2.CBORTag):
        return decode_dataframe(data.value)
    return data


def df2tag(data: Any) -> Any:
    """Loop over `data` items and encode dataframes to tag values."""
    if isinstance(data, tuple):
        lst: List[Any] = []
        for idx, val in enumerate(data):
            if isinstance(val, pl.DataFrame) or isinstance(val, pd.DataFrame):
                lst.append(encode_dataframe(val))
            else:
                lst.append(val)
        return lst
    elif isinstance(data, list):
        for idx, val in enumerate(data):
            if isinstance(val, pl.DataFrame) or isinstance(val, pd.DataFrame):
                data[idx] = encode_dataframe(val)

    elif isinstance(data, pl.DataFrame) or isinstance(data, pd.DataFrame):
        data = encode_dataframe(data)
    return data


def rsa_private_key():
    """Generate a new RSA private key."""
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def ecdsa_private_key():
    """Generate a new ECDSA private key using SECP256R1 curve."""
    return ec.generate_private_key(ec.SECP256R1(), default_backend())


def pem_public_key(private_key: PrivateKeyTypes) -> bytes:
    """Return the public key in PEM format from a private key."""
    return private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )


def save_private_key(cid: str, private_key: PrivateKeyTypes):
    """Save the private key to a file in the rembus directory."""
    kdir = os.path.join(rs.rembus_dir(), cid)

    if not os.path.exists(kdir):
        os.makedirs(kdir)

    fn = os.path.join(kdir, ".secret")
    private_key_file = open(fn, "wb")

    pem_private_key = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()
    )

    private_key_file.write(pem_private_key)
    private_key_file.close()


def load_private_key(cid: str) -> PrivateKeyTypes:
    """Load the private key from a file in the rembus directory."""
    fn = os.path.join(rs.rembus_dir(), cid, ".secret")
    # fn = os.path.join(rembus_dir(), cid)
    with open(fn, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(), password=None)

    return private_key


def load_public_key(router, cid: str):
    """Load the public key from a file in the rembus directory."""
    fn = rs.key_file(router.id, cid)
    try:
        with open(fn, "rb") as f:
            public_key = serialization.load_pem_public_key(
                f.read(),
            )
    except ValueError:
        try:
            with open(fn, "rb") as f:
                public_key = serialization.load_der_public_key(
                    f.read(),
                )
        except ValueError as e:
            raise ValueError(
                f"Could not load public key from file: {fn}") from e

    return public_key


def save_pubkey(router_id: str, cid: str, pubkey: bytes, keytype: int):
    """Save the public key to a file in the rembus directory."""
    name = rs.key_base(router_id, cid)
    keyformat = "der"
    # check if pubkey start with -----BEGIN chars
    if pubkey[0:10] == bytes(
        [0x2d, 0x2d, 0x2d, 0x2d, 0x2d, 0x42, 0x45, 0x47, 0x49, 0x4e]
    ):
        keyformat = "pem"

    if keytype == SIG_RSA:
        typestr = "rsa"
    else:
        typestr = "ecdsa"

    fn = f"{name}.{typestr}.{keyformat}"
    with open(fn, "wb") as f:
        f.write(pubkey)


def remove_pubkey(router, cid: str):
    """Remove the public key file for a component."""
    fn = rs.key_file(router.id, cid)
    os.remove(fn)
