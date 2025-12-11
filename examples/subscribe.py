import asyncio
import logging
import signal
import rembus
import pandas as pd

logging.basicConfig(encoding="utf-8", level=logging.INFO)


def receive_signal(handle, loop):
    asyncio.run_coroutine_threadsafe(handle.shutdown(), loop)


async def foo(arg):
    print(f"[foo]: {arg}")


async def mytopic(msg):
    if isinstance(msg, pd.DataFrame):
        print(f"recv dataframe:\n{msg}")
    else:
        print(f"recv {type(msg)} message: {msg}")


async def main():
    handle = await rembus.component("sub_a")
    signal.signal(
        signal.SIGINT,
        lambda signum, frame: receive_signal(
            handle, asyncio.get_running_loop()
        ),
    )

    await handle.subscribe(mytopic, True)
    await handle.subscribe(foo, False)
    await handle.wait()


if __name__ == "__main__":
    asyncio.run(main())
