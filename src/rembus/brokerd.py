"""Broker daemon for rembus."""

import asyncio
import logging
import os
from rembus.settings import DEFAULT_BROKER, DEFAULT_PORT
from rembus.protocol import CBOR, JSON
from rembus.component import _component


async def broker(
    name: str | None = None,
    port: int | None = None,
    secure: bool = False,
    policy: str = "first_up",
    schema: str | None = None,
    enc: int = CBOR,
    keyspace: bool = True,
    mqtt: str | None = None,
):
    """Initialize a rembus broker."""
    handle = await _component(
        None, name, port, secure, policy, schema, enc, keyspace, mqtt
    )
    handle.register_shutdown()
    await handle.wait()


def main():
    """Start a broker daemon."""

    name = os.environ.get("REMBUS_NAME", DEFAULT_BROKER)
    port = int(os.environ.get("REMBUS_PORT", DEFAULT_PORT))

    secure = os.environ.get("REMBUS_SECURE", "0") == "1"

    # REMBUS_MQTT_URL is used by the broker to connect to an MQTT bridge.
    # example: "mqtt://localhost:1883"
    mqtt = os.environ.get("REMBUS_MQTT_URL", None)

    # REMBUS_SCHEMA is used by the broker to load a schema file at startup.
    # The schema file is used to persist the messages received by the broker
    # into the tables defined by the schema.
    # The schema file should be in JSON format and follow the schema definition
    # used by Rembus.
    # If the schema file is not found or is invalid, the broker will start
    # without a schema.
    # REMBUS_SCHEMA can be set to an absolute file path.
    schema = os.environ.get("REMBUS_SCHEMA", None)

    policy = os.environ.get("REMBUS_POLICY", "first_up")

    keyspace = os.environ.get("REMBUS_KEYSPACE", "0") == "1"

    enc_map = {
        "CBOR": CBOR,
        "JSON": JSON,
    }

    enc_str = os.getenv("REMBUS_ENC", "CBOR").upper()
    enc = enc_map.get(enc_str, CBOR)

    log_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    log_str = os.getenv("REMBUS_LOG", "ERROR").upper()
    log_level = log_map.get(log_str)

    logging.basicConfig(
        encoding="utf-8",
        level=log_level,
        format="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.getLogger("websockets").setLevel(logging.WARNING)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(
        broker(
            name=name,
            port=port,
            secure=secure,
            policy=policy,
            mqtt=mqtt,
            schema=schema,
            keyspace=keyspace,
            enc=enc,
        )
    )
