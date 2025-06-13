"""
A component that publish dataframes metrics.
"""
import asyncio
import pandas as pd
import rembus


async def main():
    """
    Publish a dataframe.
    The published message is a one-way interschange: no values are returned.
    """

    rb = await rembus.component("python_node")

    df = pd.DataFrame({
        "metric": ["Temperature", "Pressure", "Humidity"],
        "value": [21.0, 0.99, 80],
    })

    await rb.publish("metrics", df)
    await rb.close()


loop = asyncio.new_event_loop()
loop.run_until_complete(main())
