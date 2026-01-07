import argparse
import polars as pl
import rembus as rb

parser = argparse.ArgumentParser(description="campobase broker")
parser.add_argument("-p", "--port", default=8000, help="broker port")
args = parser.parse_args()

df = pl.DataFrame(
    {
        "dn": ["mysite.sensor1", "mysite.sensor2"],
        "temperature": [15.0, 18.8],
        "pressure": [900, 1020],
    }
)

cli = rb.node(f"ws://:{args.port}/myclient")
cli.publish("telemetry", df, slot=rb.nowbucket())
cli.close()
