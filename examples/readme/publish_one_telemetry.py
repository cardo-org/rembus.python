import argparse
import time
import rembus as rb

now = time.time()

parser = argparse.ArgumentParser(description="campobase broker")
parser.add_argument("-p", "--port", default=8000, help="broker port")
parser.add_argument(
    "-T", "--temperature", default=21.0, help="temperature reading"
)
parser.add_argument("-P", "--pressure", default=1000, help="pressure reading")

parser.add_argument(
    "-b", "--bucket", default=5, help="time bucket width (minutes)"
)
args = parser.parse_args()

time_bucket = now - (now % (args.bucket * 60))

cli = rb.node(f"ws://:{args.port}/myclient")
cli.publish(
    "mysite.sensor1/telemetry",
    {"temperature": args.temperature, "pressure": args.pressure},
    slot=time_bucket,
)

cli.close()
