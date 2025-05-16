import asyncio
import logging
import pandas as pd
import pyarrow as pa
import rembus.component as rb
import sys
import os
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

# https://dev.to/aaronktberry/generating-encrypted-key-pairs-in-python-69b

logging.basicConfig(encoding="utf-8", level=logging.INFO)

pin = "11223344"
client_name = "python_app"
if len(sys.argv) > 1:
    client_name = sys.argv[1]


async def main():
    handle = await rb.uid()

    try:
        response = await handle.register(client_name, pin)
        print(response)
    except Exception as e:
        print("error:", e)
        raise
    await handle.close()


loop = asyncio.new_event_loop()
loop.run_until_complete(main())
