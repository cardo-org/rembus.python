import asyncio
from websockets import connection
from functools import partial
import logging
import rembus
import pandas as pd
import signal

logging.basicConfig(encoding="utf-8", level=logging.INFO)

def receiveSignal(handle):
    print('rembus done')
    handle.close()

def mytopic(msg):
    global counter
    if isinstance(msg, pd.DataFrame):
        print(f"recv dataframe:\n{msg}")
    else:
        print(f"recv {type(msg)} message: {msg}")


def main():
    handle = rembus.node("sub_a")
    signal.signal(
        signal.SIGINT,
        lambda signum, frame: receiveSignal(handle),
    )

    handle.subscribe(mytopic, True)
    handle.wait()


if __name__ == "__main__":
    main()
