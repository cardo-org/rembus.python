import os
from enum import Enum

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
STS_IDENTIFICATION_ERROR = 0X14 # 20
STS_METHOD_EXCEPTION = 0X28     # 40
STS_METHOD_ARGS_ERROR = 0X29    # 41
STS_METHOD_NOT_FOUND= 0X2A     # 42
STS_METHOD_UNAVAILABLE= 0X2B   # 43
STS_METHOD_LOOPBACK= 0X2C      # 44
STS_TARGET_NOT_FOUND= 0X2D     # 45
STS_TARGET_DOWN= 0X2E          # 46
STS_UNKNOWN_ADMIN_CMD= 0X2F    # 47
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

class AdminMsg:
    def __init__(self, twin, payload:list):
        self.id = payload[1]
        self.topic = payload[2]
        self.data = payload[3]
        self.twin = twin

    def __str__(self):
        return f'AdminMsg:{self.topic}: {self.data}'

class IdentityMsg:
    def __init__(self, twin, payload:list):
        self.id = payload[1]
        self.cid = payload[2]
        self.twin = twin

    def __str__(self):
        return f'IdentityMsg:{self.cid}'

class AttestationMsg:
    def __init__(self, twin, payload:list):
        self.id = payload[1]
        self.cid = payload[2]
        self.signature = payload[3]
        self.twin = twin

    def __str__(self):
        return f'AttestationMsg:{self.cid}'

class RegisterMsg:
    def __init__(self, twin, payload:list):
        self.id = payload[1]
        self.cid = payload[2]
        self.pubkey = payload[3]
        self.type = payload[4]
        self.twin = twin

    def __str__(self):
        return f'RegisterMsg:{self.cid}'
    
class UnregisterMsg:
    def __init__(self, twin, payload:list):
        self.id = payload[1]
        self.twin = twin

    def __str__(self):
        return f'UnregisterMsg:{self.twin}'    