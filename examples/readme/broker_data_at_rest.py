import asyncio
import logging
import os
import argparse
import rembus as rb

logging.basicConfig(encoding="utf-8", level=logging.INFO)
logging.getLogger("websockets").setLevel(logging.WARNING)


async def main():
    schema_file = os.path.join(os.path.dirname(__file__), "schema.json")
    parser = argparse.ArgumentParser(description="rembus broker")
    parser.add_argument("-p", "--port", default=8000, help="broker port")
    args = parser.parse_args()

    bro = await rb.component(schema=schema_file, port=args.port)

    print(f"broker up and running at port {args.port}")
    await bro.wait()


if __name__ == "__main__":
    asyncio.run(main())
