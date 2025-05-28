import asyncio
import logging
import sys
import rembus

logging.basicConfig(encoding='utf-8', level=logging.INFO)

client_name = "pub_1"
if len(sys.argv) > 1:
    client_name = sys.argv[1]


async def main():
    handle = await rembus.component(client_name)

    try:
        response = await handle.direct("main", "mymethod", 4)
        print(response)
    except Exception as e:
        print("error:", e)
    await handle.close()

loop = asyncio.new_event_loop()
loop.run_until_complete(main())
