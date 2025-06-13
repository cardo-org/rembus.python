"""
Expose the RPC service add_columns that expects a dataframe
as input and returns a modified dataframe.
"""
import asyncio
import pandas as pd
import rembus


async def add_columns(df):
    """
    Add two columns to the input dataframe and returns the result.
    The input dataframe must have `x` and `name` columns.
    """
    df["y"] = df["x"]**2
    df["descr"] = df["name"].apply(
        lambda n: f"descr for {n}" if pd.notna(n) else None)
    return df


async def metrics(df):
    """
    Handle a dataframe of metrics.
    This is handled as a one-way pubsub topic: there is no a return value
    towards the client.
    Currently the python implementation does not broadcast the published
    message to all subscribed components. To rely on the broker functionality
    a julia Rembus component is needed.
    """
    print(f"metrics:\n{df}")


async def serve():
    """Start the Rembus server"""
    rb = await rembus.component(port=8000)

    # Make available the RPC service `add_columns` to the clients.
    await rb.expose(add_columns)

    # Register to a metrics pubsub topic.
    await rb.subscribe(metrics)

    # main loop: wait for client requests.
    await rb.wait()


if __name__ == "__main__":
    asyncio.run(serve())
