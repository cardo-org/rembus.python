import os

TYPE_IDENTITY = 0
TYPE_PUB = 1
TYPE_RPC = 2
TYPE_ADMIN = 3
TYPE_RESPONSE = 4
TYPE_ACK = 6
TYPE_REGISTER = 10
TYPE_ATTESTATION = 11

OK = 0
ERROR = 0x0A
CHALLENGE = 0x0B            # 11
IDENTIFICATION_ERROR = 0X14 # 20
METHOD_EXCEPTION = 0X28     # 40
METHOD_ARGS_ERROR = 0X29    # 41
METHOD_NOT_FOUND = 0X2A     # 42
METHOD_UNAVAILABLE = 0X2B   # 43
METHOD_LOOPBACK = 0X2C      # 44
TARGET_NOT_FOUND = 0X2D     # 45
TARGET_DOWN = 0X2E          # 46
UNKNOWN_ADMIN_CMD = 0X2F    # 47

DATAFRAME_TAG = 80

retcode = {
    OK: 'OK',
    ERROR: 'Unknown error',
    IDENTIFICATION_ERROR: 'IDENTIFICATION_ERROR',
    METHOD_EXCEPTION: 'METHOD_EXCEPTION',
    METHOD_ARGS_ERROR: 'METHOD_ARGS_ERROR',
    METHOD_NOT_FOUND: 'METHOD_NOT_FOUND',
    METHOD_UNAVAILABLE: 'METHOD_UNAVAILABLE',
    METHOD_LOOPBACK: 'METHOD_LOOPBACK',
    TARGET_NOT_FOUND: 'TARGET_NOT_FOUND',
    TARGET_DOWN: 'TARGET_DOWN',
    UNKNOWN_ADMIN_CMD: 'UNKNOWN_ADMIN_CMD',
}

BROKER_CONFIG = '__config__'
COMMAND = 'cmd'
ADD_INTEREST = 'add_interest'
REMOVE_INTEREST = 'remove_interest'
ADD_IMPL = 'add_impl'
REMOVE_IMPL = 'remove_impl'

class RembusException(Exception):
    pass

class RembusTimeout(RembusException):
    def __str__(self):
        return 'request timeout'

class RembusConnectionClosed(RembusException):
    def __str__(self):
        return 'connection closed'

class RembusError(RembusException):
    def __init__(self, status_code, msg=None):
        self.status = status_code
        self.message = msg

    def __str__(self):
        if self.message:
            return f'{retcode[self.status]}:{self.message}'
        else:
            return f'{retcode[self.status]}'

def request_timeout():
    return os.environ.get('REMBUS_TIMEOUT', 3)


def msg_status(response):
    return response[2]

def msg_id(response):
    return response[1]

