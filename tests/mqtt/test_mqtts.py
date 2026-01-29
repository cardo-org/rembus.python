"""
Tests for MQTT over TLS (MQTTS) connections in rembus.
"""

import asyncio
import json
import os
import shutil
import socket
import ssl
import subprocess
import time
import pytest
from gmqtt import Client as MQTTClient
import rembus
import rembus.settings

mqtt_host = os.environ.get("MQTT_HOST", "127.0.0.1")
mqtt_port = int(os.environ.get("MQTTS_PORT", "8883"))


def wait_for_port(port, timeout=5):
    """Wait for tcp listener port up"""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((mqtt_host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"Port {port} not available")


def configure_tls_material():
    """
    Install CA + cert + key into the standard rembus locations.
    """
    data_dir = os.path.join("tests", "cfg")
    ca_dir = os.path.join(rembus.rembus_dir(), "ca")
    keystore_dir = rembus.settings.keystore_dir()

    os.makedirs(ca_dir, exist_ok=True)
    os.makedirs(keystore_dir, exist_ok=True)

    shutil.copy(
        os.path.join(data_dir, "rembus-ca.crt"),
        os.path.join(ca_dir, "rembus-ca.crt"),
    )
    shutil.copy(
        os.path.join(data_dir, "rembus.crt"),
        os.path.join(keystore_dir, "rembus.crt"),
    )
    shutil.copy(
        os.path.join(data_dir, "rembus.key"),
        os.path.join(keystore_dir, "rembus.key"),
    )


@pytest.fixture(scope="session")
def mosquitto_tls():
    """
    Start a Mosquitto broker configured with rembus TLS materials.
    """
    configure_tls_material()
    ca_dir = os.path.join(rembus.rembus_dir(), "ca")
    keystore_dir = rembus.settings.keystore_dir()
    ca = os.path.join(ca_dir, "rembus-ca.crt")
    cert = os.path.join(keystore_dir, "rembus.crt")
    key = os.path.join(keystore_dir, "rembus.key")
    conf = os.path.join(rembus.rembus_dir(), "mosquitto-tls.conf")
    with open(conf, "w", encoding="utf-8") as f:
        f.write(f"""
listener 8883
protocol mqtt
cafile {ca}
certfile {cert}
keyfile {key}
allow_anonymous true
""")

    proc = subprocess.Popen(
        ["mosquitto", "-c", conf, "-v"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    wait_for_port(mqtt_port)
    yield
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture
def mqtt_ssl_context():
    """
    SSL context used by the gmqtt test subscriber.
    It is the context used by Rembus.
    """
    ctx = ssl.create_default_context(
        cafile=os.path.join("tests", "cfg", "rembus-ca.crt")
    )
    return ctx


@pytest.mark.asyncio
async def test_mqtts_publish(mosquitto_tls, mqtt_ssl_context):
    """
    Verify that rembus can publish messages over MQTT+TLS
    and that a TLS-authenticated gmqtt client can receive them.
    """
    received = asyncio.Event()
    payload_out = {}

    async def on_message(client, topic, payload, qos, properties):
        payload_out["topic"] = topic
        payload_out["data"] = json.loads(payload.decode())
        received.set()

    # Start Rembus broker component with a MQTTS bridge.
    broker = await rembus.component(
        mqtt=f"mqtts://{mqtt_host}:{mqtt_port}",
        port=8000,
    )

    # TLS subscriber
    sub = MQTTClient("mqtts-test-subscriber", clean_session=True)
    sub.on_message = on_message
    await sub.connect(mqtt_host, mqtt_port, ssl=mqtt_ssl_context)
    sub.subscribe("#")

    await asyncio.sleep(0.1)  # allow SUBSCRIBE to settle

    # Publisher via rembus
    pub = await rembus.component("publisher")
    await pub.publish("secure/topic", {"value": 42})

    await asyncio.wait_for(received.wait(), timeout=3)

    assert payload_out["topic"] == "secure/topic"
    assert payload_out["data"] == {"value": 42}

    await sub.disconnect()
    await pub.close()
    await broker.close()
