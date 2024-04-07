import cbor2
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization
import logging
import os
import pandas as pd
from platformdirs import *
import pyarrow as pa
import ssl
from urllib.parse import urlparse
import uuid
import websockets

from .protocol import *

# create logger
logger = logging.getLogger("rembus")


def tohex(bytes):
    """Return a string with bytes as hex numbers with 0xNN format."""
    return ' '.join(f'0x{x:02x}' for x in bytes)


def field_repr(bstr):
    """String repr of the second field of a rembus message.

    The second field may be a 16-bytes message unique id or a topic string
    value.  
    """
    return tohex(bstr) if isinstance(bstr, bytes) else bstr


def msg_str(dir, msg):
    """Return a printable dump of rembus message `msg`."""
    payload = ", ".join(str(el) for el in msg[2:])
    s = f'{dir}: [{msg[0]}, {field_repr(msg[1])}, {payload}]'
    return s


class Component:

    def __init__(self, url=None) -> None:
        uri = urlparse(url)
        if uri.path:
            self.name = uri.path[1:] if uri.path.startswith("/") else uri.path
        else:
            self.name = None
        if uri.scheme:
            self.scheme = uri.scheme
        else:
            self.scheme = "ws"
        if uri.netloc:
            self.netloc = uri.netloc
        else:
            self.netloc = os.getenv('REMBUS_BASE_URL', 'localhost:8000')

    def connection_url(self):
        if self.name:
            return f"{self.scheme}://{self.netloc}/{self.name}"
        else:
            return f"{self.scheme}://{self.netloc}"


def decode_dataframe(data):
    """Decode a CBOR tagged value `data` to a pandas dataframe."""
    writer = pa.BufferOutputStream()
    writer.write(data)
    buf = writer.getvalue()
    reader = pa.ipc.open_stream(buf)
    with pa.ipc.open_stream(buf) as reader:
        return reader.read_pandas()


def encode_dataframe(df):
    """Encode a pandas dataframe `df` to a CBOR tag value."""
    table = pa.Table.from_pandas(df)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write(table)
    buf = sink.getvalue()
    stream = pa.input_stream(buf)
    return cbor2.CBORTag(DATAFRAME_TAG, stream.read())


def encode(msg):
    """Encode message `msg`."""
    logger.debug(msg_str('out', msg))
    return cbor2.dumps(msg)


def tag2df(data):
    """Loop over `data` items and decode tagged values to dataframes."""
    if isinstance(data, list):
        for idx, val in enumerate(data):
            if isinstance(val, cbor2.CBORTag) and val.tag == DATAFRAME_TAG:
                data[idx] = decode_dataframe(val.value)
    elif isinstance(data, cbor2.CBORTag):
        return decode_dataframe(data.value)
    return data


def df2tag(data):
    """Loop over `data` items and encode dataframes to tag values."""
    if isinstance(data, tuple):
        lst = []
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


def id():
    """Return an array of 16 random bytes."""
    return uuid.uuid4().bytes


def config_dir():
    """The directory for rembus secrets."""
    # appears in path only for Windows machine
    app_author = "Rembus"
    return user_config_dir("rembus", app_author)


def save_private_key(cid, private_key):
    dir = config_dir()
    if not os.path.exists(dir):
        os.makedirs(dir)

    fn = os.path.join(dir, cid)
    private_key_file = open(fn, "wb")
    private_key_file.write(private_key)
    private_key_file.close()


def load_private_key(cid):
    fn = os.path.join(config_dir(), cid)
    with open(fn, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(), password=None)

    return private_key
