"""
RPC client that shows dataframes exchanges.
"""
import asyncio
import pandas as pd
import rembus


async def main():
    """
    Send a dataframe and expect a dataframe as the result
    of RPC `add_columns` service.
    """

    rb = await rembus.component("python_node")

    df = pd.DataFrame({
        "name": ["abc", None, "mylabel"],
        "x": [1.0, float("inf"), 300],
    })

    target_df = await rb.rpc("add_columns", df)
    print(f"target_df:\n{target_df}")

    await rb.close()


loop = asyncio.new_event_loop()
loop.run_until_complete(main())
