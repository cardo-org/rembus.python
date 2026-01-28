import asyncio
import json
import logging
import os
import pytest
import socket
import time
from gmqtt import Client as MQTTClient
import rembus as rb

mqtt_host = os.environ.get("MQTT_HOST", "127.0.0.1")
mqtt_port = int(os.environ.get("MQTT_PORT", "1883"))


def wait_for_broker(port):
    """Wait until the MQTT broker accepts connections."""

    for _ in range(20):
        try:
            s = socket.create_connection((mqtt_host, port), timeout=1)
            s.close()
            return
        except OSError:
            time.sleep(0.5)
    raise RuntimeError(f"MQTT broker {mqtt_host}:{port} not ready")


async def publish_mqtt_message(topic, payload):
    client = MQTTClient("test-publisher")
    await client.connect("localhost", 1883)

    client.publish(topic, json.dumps(payload), qos=0)

    # give broker time to deliver
    await asyncio.sleep(0.2)

    await client.disconnect()


async def publish_wrong_payload():
    """The payload is not a valid JSON."""
    client = MQTTClient("test-publisher")
    await client.connect("localhost", 1883)
    data = "This is not a JSON"
    client.publish("mqtt_topic", data, qos=0)
    # give broker time to deliver
    await asyncio.sleep(0.2)
    await client.disconnect()


def test_mqtt_subscribe():
    received = asyncio.Event()

    def mqtt_topic(message):
        logging.debug("Received MQTT message: %s", message)
        received.set()

    wait_for_broker(mqtt_port)

    bro = rb.node(mqtt=f"mqtt://{mqtt_host}:{mqtt_port}", port=8000)

    cli = rb.node("mysubscriber")
    cli.subscribe(mqtt_topic)

    asyncio.run(publish_wrong_payload())

    # publish a mqtt message using a MQTT client.
    payload = {"name": "rembus", "value": 42}
    asyncio.run(publish_mqtt_message("mqtt_topic", payload))

    asyncio.run(asyncio.wait_for(received.wait(), timeout=2))

    cli.close()
    bro.close()


def consume_list(topic, x, y):
    logging.debug("consume_list %s: x=%s, y=%s", topic, x, y)


def test_mqtt_space_subscribe():
    received = asyncio.Event()

    def consume_alarms(topic, data):
        logging.debug("consume_alarms %s with data: %s", topic, data)
        received.set()

    bro = rb.node(mqtt=f"mqtt://{mqtt_host}:{mqtt_port}", port=8000)

    cli = rb.node("mysubscriber")
    cli.subscribe(consume_alarms, topic="*/alarm")
    cli.subscribe(consume_list, topic="*/sequence")

    asyncio.run(publish_mqtt_message("home/alarm", {"status": "on"}))

    # if the payload is a list the elements become arguments of
    # the subscribed callback, see consume_list signature.
    asyncio.run(publish_mqtt_message("home/sequence", [1, 2]))

    asyncio.run(asyncio.wait_for(received.wait(), timeout=2))

    cli.close()
    bro.close()


@pytest.mark.asyncio
async def test_mqtt_publish():
    received = asyncio.Event()
    received_payload = {}

    async def on_message(client, topic, payload, qos, properties):
        logging.debug(
            "received MQTT message on topic %s: %s",
            topic,
            payload,
        )
        received_payload["topic"] = topic
        received_payload["data"] = json.loads(payload.decode())
        received.set()

    bro = await rb.component(mqtt=f"mqtt://{mqtt_host}:{mqtt_port}", port=8000)

    # --- MQTT subscriber
    sub = MQTTClient("mqtt-subscriber")
    sub.on_message = on_message
    await sub.connect("localhost", 1883)
    sub.subscribe("#", qos=0)

    cli = await rb.component("mypublisher")

    await cli.publish("mqtt_topic", {"value": 42})
    await asyncio.wait_for(received.wait(), timeout=2)
    assert received_payload["topic"] == "mqtt_topic"
    assert received_payload["data"] == {"value": 42}

    received.clear()
    await cli.publish("a/b/c", 1, 2)

    await asyncio.wait_for(received.wait(), timeout=2)
    assert received_payload["topic"] == "a/b/c"
    assert received_payload["data"] == [1, 2]

    await sub.disconnect()
    await cli.close()
    await bro.close()
