import os
from enum import Enum

WS_FRAME_MAXSIZE = 60 * 1024 * 1024

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
STS_IDENTIFICATION_ERROR = 0X14 # 20
STS_METHOD_EXCEPTION = 0X28     # 40
STS_METHOD_ARGS_ERROR = 0X29    # 41
STS_METHOD_NOT_FOUND= 0X2A     # 42
STS_METHOD_UNAVAILABLE= 0X2B   # 43
STS_METHOD_LOOPBACK= 0X2C      # 44
STS_TARGET_NOT_FOUND= 0X2D     # 45
STS_TARGET_DOWN= 0X2E          # 46
STS_UNKNOWN_ADMIN_CMD= 0X2F    # 47

DATAFRAME_TAG = 80

retcode = {
    STS_OK: 'OK',
    STS_ERROR: 'Unknown error',
    STS_IDENTIFICATION_ERROR: 'IDENTIFICATION_ERROR',
    STS_METHOD_EXCEPTION: 'METHOD_EXCEPTION',
    STS_METHOD_ARGS_ERROR: 'METHOD_ARGS_ERROR',
    STS_METHOD_NOT_FOUND: 'METHOD_NOT_FOUND',
    STS_METHOD_UNAVAILABLE: 'METHOD_UNAVAILABLE',
    STS_METHOD_LOOPBACK: 'METHOD_LOOPBACK',
    STS_TARGET_NOT_FOUND: 'TARGET_NOT_FOUND',
    STS_TARGET_DOWN: 'TARGET_DOWN',
    STS_UNKNOWN_ADMIN_CMD: 'UNKNOWN_ADMIN_CMD',
}

BROKER_CONFIG = '__config__'
COMMAND = 'cmd'
ADD_INTEREST = 'subscribe'
REMOVE_INTEREST = 'unsubscribe'
ADD_IMPL = 'expose'
REMOVE_IMPL = 'unexpose'

def id():
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
    pass

class RembusTimeout(RembusException):
    def __str__(self):
        return 'request timeout'

class RembusConnectionClosed(RembusException):
    def __str__(self):
        return 'connection down'

class RembusError(RembusException):
    def __init__(self, status_code:int, msg:str|None=None):
        self.status = status_code
        self.message = msg

    def __str__(self):
        if self.message:
            return f'{retcode[self.status]}:{self.message}'
        else:
            return f'{retcode[self.status]}'

