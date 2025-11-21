import asyncio
import rembus


async def add(x, y):
    return x + y


async def main():
    rb = await rembus.component(port=8000)

    await rb.expose(add)

    await rb.wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nbye")
