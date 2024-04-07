import asyncio
import logging
import rembus

logging.basicConfig(encoding="utf-8", level=logging.INFO)


async def main():
    rb = await rembus.component('pappo')
    print(await rb.rpc('version'))
    await rb.close()

asyncio.new_event_loop().run_until_complete(main())
