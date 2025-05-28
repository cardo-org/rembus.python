import asyncio
import logging
import signal
import rembus

logging.basicConfig(encoding="utf-8", level=logging.INFO)


def receiveSignal(handle, loop):
    asyncio.run_coroutine_threadsafe(handle.close(), loop)


async def add_column(df):
    df["from_python"] = [f"name{i}" for i in df.index]
    print(f"add_column: {df}")
    return df


async def main():
    handle = await rembus.component("df_transformer")
    signal.signal(
        signal.SIGINT,
        lambda signum, frame: receiveSignal(
            handle, asyncio.get_running_loop()),
    )

    await handle.expose(add_column)
    await handle.wait()


if __name__ == "__main__":
    asyncio.run(main())
