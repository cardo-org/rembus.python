import asyncio
from websockets import connection
from functools import partial
import logging
import rembus
import pandas as pd
import signal

logging.basicConfig(encoding="utf-8", level=logging.INFO)

def receiveSignal(handle, loop):
    asyncio.run_coroutine_threadsafe(handle.close(), loop)

async def foo(arg):
    print(f'[foo]: {arg}')

async def mytopic(msg):
    global counter
    if isinstance(msg, pd.DataFrame):
        print(f"recv dataframe:\n{msg}")
    else:
        print(f"recv {type(msg)} message: {msg}")


async def main():
    handle = await rembus.component("sub_a")
    signal.signal(
        signal.SIGINT,
        lambda signum, frame: receiveSignal(handle, asyncio.get_running_loop()),
    )

    await handle.subscribe(mytopic, True)
    await handle.subscribe(foo, False)
    await handle.forever()


if __name__ == "__main__":
    asyncio.run(main())
