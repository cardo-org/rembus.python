import asyncio
import logging
import pandas as pd
import pyarrow as pa
import rembus
import sys
import time

logging.basicConfig(encoding="utf-8", level=logging.INFO)

client_name = "python_app"
if len(sys.argv) > 1:
    client_name = sys.argv[1]


async def main():
    handle = await rembus.component(client_name)

    df = pd.DataFrame({
                        "a": [1.0, float("inf"), 300],
                        "label": ["abc", None, "mylabel"]
                      })
    
    for i in range(1, 2):
        await handle.publish("mytopic", df)

    await handle.close()


loop = asyncio.new_event_loop()
loop.run_until_complete(main())
