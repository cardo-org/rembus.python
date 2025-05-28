import logging
import sys
import pandas as pd
import rembus.sync as rembus

logging.basicConfig(encoding="utf-8", level=logging.INFO)

client_name = "python_app"
if len(sys.argv) > 1:
    client_name = sys.argv[1]


def main():
    rb = rembus.node(client_name)

    # df = pd.DataFrame({"a": [1.0, float("inf"), 999], "label": ["abc", None, "xxx"]})
    df = pd.DataFrame({"a": [1, 2]})

    rb.publish("mytopic", df)
    rb.publish("foo", df)
    rb.close()


main()
