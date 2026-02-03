import asyncio
import logging
import argparse
import rembus as rb

logging.basicConfig(encoding="utf-8", level=logging.INFO)
logging.getLogger("websockets").setLevel(logging.WARNING)


async def main():
    parser = argparse.ArgumentParser(description="rembus broker")
    parser.add_argument("-p", "--port", default=8000, help="broker port")
    args = parser.parse_args()

    bro = await rb.component(port=args.port)

    # Add SIGINT and SIGTERM signal handlers for controlled shutdown.
    bro.register_shutdown()

    print(f"broker up and running at port {args.port}")
    await bro.wait()


if __name__ == "__main__":
    asyncio.run(main())
