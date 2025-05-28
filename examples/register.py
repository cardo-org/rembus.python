import logging
import rembus
import sys

# https://dev.to/aaronktberry/generating-encrypted-key-pairs-in-python-69b

logging.basicConfig(encoding="utf-8", level=logging.INFO)

pin = "11223344"
client_name = "python_app"
if len(sys.argv) > 1:
    client_name = sys.argv[1]


def main():
    try:
        rembus.register(client_name, pin)
    except Exception as e:
        print("error:", e)
        raise
