#!/usr/bin/env python3
"""
Rembus tutorial example: remote Python function deployment and execution.

Prerequisite:
    A running rembus broker, for example:
    > python example/broker.py

Usage:
    python distributed.py [--install]

- --install : Deploy the `stats` function to the remote node
- No flags : Just invoke the remote `stats` RPC

This script demonstrates how to:
1. Generate a synthetic time-series dataset using Polars
2. Define a Python function (`stats`) that operates on a DataFrame
3. Attempt to call the function remotely via Rembus
4. Dynamically deploy the function source code to a remote node
5. Invoke the deployed function and retrieve the result

The example mimics a telemetry/monitoring use case with sensor metrics.
"""

import argparse
import random
from datetime import datetime, timedelta
import polars as pl
import rembus as rb

stats_src = """
import polars as pl

def stats(df):
    return (
        df.group_by("kpi")
        .agg(
            [
                pl.col("value").mean().alias("mean"),
                pl.col("value").min().alias("min"),
                pl.col("value").median().alias("median"),
                pl.col("value").max().alias("max"),
                pl.count().alias("count"),
            ]
        )
        .sort("kpi")
    )
"""


def create_df(n_rows) -> pl.DataFrame:
    """
    Create a synthetic telemetry dataset.

    The dataset simulates multiple sensors producing different KPIs
    (temperature, pressure, humidity) over time.
    """
    sensors = [f"sensor_{i}" for i in range(1, 11)]
    kpis = ["temperature", "pressure", "humidity"]
    start_ts = datetime(2026, 1, 1)

    data = {
        "name": [],
        "kpi": [],
        "ts": [],
        "value": [],
    }

    for i in range(n_rows):
        sensor = random.choice(sensors)
        kpi = random.choice(kpis)

        if kpi == "temperature":
            value = random.uniform(15.0, 35.0)
        elif kpi == "pressure":
            value = random.uniform(980.0, 1050.0)
        else:  # humidity
            value = random.uniform(20.0, 90.0)

        ts = start_ts + timedelta(minutes=i)
        data["name"].append(sensor)
        data["kpi"].append(kpi)
        data["ts"].append(ts.isoformat())
        data["value"].append(round(value, 2))

    print("DONE")
    return pl.DataFrame(data)


def main():
    """
    Main application entry point.

    This function:
    1. Connects to a Rembus node
    2. Generates a dataset
    3. Tries to invoke a remote function that is not yet installed
    4. Deploys the function source code remotely
    5. Calls the function again and prints the result
    """
    parser = argparse.ArgumentParser(description="Rembus distributed example")
    parser.add_argument(
        "--install",
        action="store_true",
        help="Install the stats service on the remote node",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
        help="Number of rows to generate in the datframe (default: 1000)",
    )
    args = parser.parse_args()

    # Connect to a Rembus node
    cli = rb.node("myapp")

    # Create local dataset
    df = create_df(args.rows)

    if args.install:
        # Deploy the Python function as a remote service
        res = cli.rpc(
            "python_service_install",  # software distribution topic
            "stats",  # remote RPC service name
            stats_src,  # function source code
        )
        print(f"Service installation result: {res}")

    # Invoke the newly installed remote function
    result = cli.rpc("stats", df)
    print(f"Metrics summary:\n{result}")

    # Close the Rembus connection
    cli.close()


if __name__ == "__main__":
    main()
