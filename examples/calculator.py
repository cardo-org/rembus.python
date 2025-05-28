import asyncio
import rembus


async def add(x, y):
    return x+y


async def main():
    rb = await rembus.component()

    await rb.expose(add)
    await rb.wait()

loop = asyncio.new_event_loop()
loop.run_until_complete(main())
