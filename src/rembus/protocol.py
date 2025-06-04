"""
Utility functions, messages and protocol constants related
to the Rembus protocol.
"""
import os
import logging
from typing import Any, List
import cbor2
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa, ec
from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
from cryptography.hazmat.backends import default_backend
import pandas as pd
import pyarrow as pa
import rembus.settings as rs

logger = logging.getLogger(__name__)

WS_FRAME_MAXSIZE = 60 * 1024 * 1024

SIG_RSA = 0x1
SIG_ECDSA = 0x2

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
    return bytearray(os.urandom(16))


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


class AdminMsg:
    """AdminMsg packet."""

    def __init__(self, twin, payload: list):
        self.id = payload[1]
        self.topic = payload[2]
        self.data = payload[3]
        self.twin = twin

    def __str__(self):
        return f'AdminMsg:{self.topic}: {self.data}'


class IdentityMsg:
    """IdentityMsg packet.
    This message is sent by the component to identify itself
    to the remote peer.
    """

    def __init__(self, twin, payload: list):
        self.id = payload[1]
        self.cid = payload[2]
        self.twin = twin

    def __str__(self):
        return f'IdentityMsg:{self.cid}'


class AttestationMsg:
    """AttestationMsg packet.
    This message is sent by the component to authenticate
    its identity to the remote peer.
    """

    def __init__(self, twin, payload: list):
        self.id = payload[1]
        self.cid = payload[2]
        self.signature = payload[3]
        self.twin = twin

    def __str__(self):
        return f'AttestationMsg:{self.cid}'


class RegisterMsg:
    """RegisterMsg packet.
    This message is sent by the component to register
    its public key to the remote peer.
    """

    def __init__(self, twin, payload: list):
        self.id = payload[1]
        self.cid = payload[2]
        self.pubkey = payload[3]
        self.type = payload[4]
        self.twin = twin

    def __str__(self):
        return f'RegisterMsg:{self.cid}'


class UnregisterMsg:
    """UnregisterMsg packet.
    This message is sent by the component to unregister
    its public key from the remote peer.
    """

    def __init__(self, twin, payload: list):
        self.id = payload[1]
        self.twin = twin

    def __str__(self):
        return f'UnregisterMsg:{self.twin}'


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


def field_repr(bstr: bytes | str):
    """String repr of the second field of a rembus message.

    The second field may be a 16-bytes message unique id or a topic string
    value.
    """
    return tohex(bstr) if not isinstance(bstr, str) else bstr


def msg_str(direction: str, msg: list[Any]):
    """Return a printable dump of rembus message `msg`."""
    payload = ", ".join(str(el) for el in msg[2:])
    s = f'{direction}: [{msg[0]}, {field_repr(msg[1])}, {payload}]'
    return s


def decode_dataframe(data: bytes) -> pd.DataFrame:
    """Decode a CBOR tagged value `data` to a pandas dataframe."""
    writer = pa.BufferOutputStream()
    writer.write(data)
    buf: pa.Buffer = writer.getvalue()
    reader = pa.ipc.open_stream(buf)
    with pa.ipc.open_stream(buf) as reader:
        return reader.read_pandas()


def encode_dataframe(df: pd.DataFrame) -> cbor2.CBORTag:
    """Encode a pandas dataframe `df` to a CBOR tag value."""
    table = pa.Table.from_pandas(df)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write(table)
    buf = sink.getvalue()
    stream = pa.input_stream(buf)
    return cbor2.CBORTag(DATAFRAME_TAG, stream.read())


def encode(msg: list[Any]) -> bytes:
    """Encode message `msg`."""
    logger.debug(msg_str('out', msg))
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
            if isinstance(val, pd.DataFrame):
                lst.append(encode_dataframe(val))
            else:
                lst.append(val)
        return lst
    elif isinstance(data, list):
        for idx, val in enumerate(data):
            if isinstance(val, pd.DataFrame):
                data[idx] = encode_dataframe(val)

    elif isinstance(data, pd.DataFrame):
        data = encode_dataframe(data)
    return data


def regid(mid: bytearray, pin: str) -> bytearray:
    """Return a message id embedding the `pin` into `mid`."""
    bpin = bytes.fromhex(pin[::-1])
    mid[:4] = bpin[:4]
    return mid


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
