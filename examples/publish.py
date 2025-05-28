import asyncio
import rembus


async def main():
    rb = await rembus.component('foostacchio')

    await rb.publish("mytopic",
                     {
                         'name': 'sensor_1',
                         'metric': 'T',
                         'value': 21.6
                     })

    await rb.close()


loop = asyncio.new_event_loop()
loop.run_until_complete(main())
