# Rembus for Python

[![Build Status](https://github.com/cardo-org/rembus.python/actions/workflows/python-app.yml/badge.svg?branch=main)](https://github.com/cardo-org/rembus.python/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/cardo-org/rembus.python/branch/main/graph/badge.svg)](https://codecov.io/gh/cardo-org/rembus.python)

Rembus is a Pub/Sub and RPC middleware.

There are few key concepts to get confident with Rembus:

- A Component is a node of a distributed system that communicate with Pub/Sub or RPC styles;
- A Component connect to a Broker;
- A Broker dispatch messages between Components;
- A Component expose RPC services and/or subscribe to Pub/Sub topics;
- A Component make RPC requests and/or publish messages to Pub/Sub topics;
- A schema may be associated to a broker for structuring data at rest;

The Rembus python version supports only the WebSocket protocol.

See [Rembus.jl](https://cardo-org.github.io/Rembus.python/stable/) broker
for a full fledged broker that supports WebSocket, ZMQ and plain tcp protocols
and more features like private topics, multi tenancy and more.

## Getting Started

Install the package:

```shell
pip install rembus
```

## Cheatsheet

The following `schema.json` will be used in the cheatsheet:

```json
{
    "tables": [
        {
            "table": "sensor",
            "topic": ":site/:type/:dn/sensor",
            "columns": [
                {"col": "site", "type": "TEXT", "nullable": false},
                {"col": "type", "type": "TEXT", "nullable": false},
                {"col": "dn", "type": "TEXT"}
            ],
            "keys": ["dn"]
        },
        {
            "table": "telemetry",
            "topic": ":dn/telemetry",
            "columns": [
                {"col": "dn", "type": "TEXT"},
                {"col": "temperature", "type": "DOUBLE"},
                {"col": "pressure", "type": "DOUBLE"}
            ],
            "extras": {"recv_ts": "ts", "slot": "time_bucket"}
        }
    ]
}
```

### Broker start

A Rembus component has a name and connects to the broker. The `node` method is
for the sync version and the `component` method is for the async version.

`node`/`component` without the `url` argument starts a broker
component.

```python
import rembus as rb

bro = rb.node() # equivalent to rb.node(port = 8000)
bro.wait() # rembus loop, unnecessary if running in REPL interpreter 
```

```python
import rembus as rb

bro = await rb.component()
await bro.wait() 
```

### Broker with Data at Rest tables

With a data schema the DataLake DuckDB tables are created if they not exist and
published messages are persisted into.  

```python
import rembus as rb

bro = await rb.component(schema="schema.json")
await bro.wait() 
```

### Connect to a Broker

Component `myname` connects to a Broker listening on port `8000`
on host `willy.acme.org`. Only one component with name `myname` may connect to
the broker:

```python
cli = await rb.component("ws://willy.acme.org:8000/myname")
```

Component `myname` connects to a Broker listening on default port `8000`
on default host `127.0.0.1`:

```python
cli = await rb.component("myname")
```

Anonymous component to a Broker listening on port `8000`
on host `willy.acme.org`. Any number of anonymous components may connect to
the broker:

```python
cli = await rb.component("ws://willy.acme.org:8000")
```

Anonymous component connects to a Broker listening on default port `8000`
on default host `127.0.0.1`:

```python
cli = await rb.component("")
```

> **WHY ASSIGN A NAME TO A COMPONENT?**
If component is anonymous then a random identifier that changes at each
connection event is used as the component identifier. In this case the broker
is unable to bind the component to a persistent twin and messages published
when the component is offline get not broadcasted to the component when it gets
online again.

### Publish a sensor dataframe

```python
import polars as pl
import rembus as rb

df = pl.DataFrame(
    {
        "site": ["mysite", "mysite"],
        "dn":["mysite.sensor1", "mysite.sensor2"],
        "type": ["HVAC", "HVAC"]
    }
)

cli = rb.node("myclient")
cli.publish("sensor", df)
```

### Publish a telemetry dictionary

```python
import rembus as rb

dn = "mysite.sensor1" # dn: unique Distinguish Name
cli = rb.node(dn)
cli.publish(f"{dn}/telemetry", {'temperature': 21.6, 'pressure': 980})
```

```python
import asyncio
import rembus as rb

async def main():
    dn = "mysite.sensor1"
    cli = await rembus.component(dn)
    await cli.publish(f"{dn}/telemetry", {'temperature': 18, 'pressure': 770})
    await cli.close()
```

### Publish a telemetry dataframe

```python
import polars as pl
import rembus as rb

df = pl.DataFrame(
    {
        "dn":["mysite.sensor1", "mysite.sensor2"],
        "temperature": [15.0, 18.8],
        "pressure": [900, 1020],
    }
)

cli = rb.node("myclient")
cli.publish("telemetry", df)
```

### Publish to an alarm topic

```python
import rembus as rb

slogan = "mydevice: battery very low"
severity = "CRITICAL"

dev = rb.node("mydevice")
dev.publish("alarm", slogan, severity)
```

### Subscribe to a topic and declare interest in undelivered messages

The subscribed function gets called with the pubsub message payload arguments.

The option `msgfrom` setup for receiving published messages when the component
was not subscribed to the topic, for example because it was not connected.

```python
import rembus as rb

def alarm(descr, severity):
    print(f"{severity}: {descr}")

sub = rb.node("monitor", msgfrom=rb.LastReceived)
sub.subscribe(alarm)
```

### Subscribe to the telemetry space

You can subscribe to set of topics (a topic space) using a 'regular expression', all
topics matching the pattern are subscribed.

The subscribed function gets called with the originating topic as
first argument followed by the pubsub message payload arguments. 

```python
import rembus as rb

def telemetry(topic, data):
    print(f"from {topic}: do something with {data}")

sub = rb.node("collector")
sub.subscribe(telemetry, topic="*/telemetry")
```

> **NOTE**: To cache messages for an offline component the broker needs to know that
  such component has subscribed for a specific topic. This imply that messages 
  published before the first subscribe happens will be lost. If you want all message
  will be delivered subscribe first and publish after.  

### Expose a RPC service

A RPC service is implemented with a function named as the exposed service.

```python
import rembus as rb

def add(x,y):
    return x+y

handle = rb.node('calculator')

handle.expose(add)
```

### Call a RPC service

The `calculator` component exposes the `add` service, the RPC client will invoke as:

```python
import rembus as rb

handle = rb.node("myclient")
result = handle.rpc('add', 1, 2)
```

