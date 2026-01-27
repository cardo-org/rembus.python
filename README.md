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
- A Schema may be associated to a broker for structuring data at rest;

The Rembus python version supports only the WebSocket protocol.

See [Rembus.jl](https://cardo-org.github.io/Rembus.python/stable/) broker
for a full fledged broker that supports WebSocket, ZMQ and plain tcp protocols
and more features like private topics, multi tenancy and more.

## Getting Started

Install the package:

```shell
pip install rembus
```

### Broker

A Rembus Component has a name and connects to a Broker. The `node` method is
for the sync version and the `component` method is for the async version.

`node`/`component` without the `url` argument starts a Broker:

```python
# sync version
import rembus as rb

bro = rb.node() # equivalent to rb.node(port = 8000)
bro.wait() # rembus loop, unnecessary if running a REPL interpreter 
```

```python
# async version
import rembus as rb

bro = await rb.component()
await bro.wait() 
```

### Broker with Data at Rest

A Rembus Broker may be associated with a data schema for persisting published
messages into a [DuckDB](https://duckdb.org/) database using the
[DataLake](https://ducklake.select/) extension.

The following `schema.json` file will be used as reference:

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

The schema defines two tables: `sensor` and `telemetry` with their
corresponding pub/sub topics and db columns.

The `sensor` table persists messages published to topics matching the pattern
`:site/:type/:dn/sensor` where `:site`, `:type` and `:dn` are variables that
get mapped to the corresponding columns of the table:

* `site` is the sensor location;
* `type` is the sensor type;
* `dn` is the sensor unique distinguish name.

The `telemetry` table persists metrics values published to topics matching 
the pattern `:dn/telemetry`. The table has the columns:

* `dn` is the sensor unique distinguish name;
* `temperature` is the temperature value;
* `pressure` is the pressure value.

With a data schema the DataLake tables are created if they not exist and
published messages are persisted into.  

When the `schema.json` filename is passed to the `node`/`component` method
the broker is started with data at rest capabilities:

```python
import rembus as rb

bro = await rb.component(schema="schema.json")
await bro.wait() 
```

### Component

Component `myname` connects to a Broker listening on port `8000`
on host `willy.acme.org`. Only one Component with name `myname` may connect to
the Broker:

```python
cli = await rb.component("ws://willy.acme.org:8000/myname")
```

Component `myname` connects to a Broker listening on default port `8000`
on default host `127.0.0.1`:

```python
cli = await rb.component("myname")
```

Anonymous component are allowed.

To connects anonymously to a Broker listening on port `8000`
on host `willy.acme.org`:

```python
cli = await rb.component(rb.anonym(host="willy.acme.org", port=8000))
```

Anonymous Component connected on default port `8000`
on default host `127.0.0.1`:

```python
cli = await rb.component(rb.anonym())
```

### Why assign a name to a component?

These are two reasons to assign a name to a component:

1. A component without a name cannot authenticate its identity to the broker with
a signature based on RSA, ECDSA or a shared secret.

1. If a component is anonymous then a random identifier that changes at each
connection event is used as the component identifier. In this case the broker
is unable to bind the component to a persistent twin and messages published
when the component is offline get not broadcasted to the component when it gets
online again.

### Publish a single message

The component `myclient` publish to topic `sensor` a single message with an
empty payload because all the sensor fields are defined in the topic:

```python
# sync version
import rembus as rb

site = mysite
type = "HVAC"
dn = "mysite.sensor1"
cli = rb.node("myclient")
cli.publish(f"{site}/{type}/{dn}/sensor")
```

```python
# async version
import rembus as rb

site = mysite
type = "HVAC"
dn = "mysite.sensor1"

async def main():
    cli = await rb.component("myclient")
    await cli.publish(f"{site}/{type}/{dn}/sensor")
```

Publish a telemetry message to topic `mysite.sensor1/telemetry` using a
dictionary as payload:

```python
# sync version
cli.publish(f"{dn}/telemetry", {'temperature': 21.6, 'pressure': 980})
```

```python
# async version
await cli.publish(f"{dn}/telemetry", {'temperature': 21.6, 'pressure': 980})
```

### Publish a DataFrame

The component `myclient` publish to topic `sensor` a bunch of messages using a
DataFrame as payload `:

```python
# sync version
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

Publish to topic `telemetry` a bunch of metrics messages using a
DataFrame as payload `:


```python
#async version
import polars as pl
import rembus as rb

df = pl.DataFrame(
    {
        "dn":["mysite.sensor1", "mysite.sensor2"],
        "temperature": [15.0, 18.8],
        "pressure": [900, 1020],
    }
)

async  def main():
    cli = await rb.component("myclient")
    await cli.publish("telemetry", df)
```

```python
import rembus as rb

slogan = "mydevice: battery very low"
severity = "CRITICAL"

dev = rb.node("mydevice")
dev.publish("alarm", slogan, severity)
```

### Subscribe to a topic

Soppose that the `alarm` topic conveys alarm messages with two fields:
`slogan` and `severity`. 

On the alarm producer side:

```python
slogan = "mydevice: battery very low"
severity = "CRITICAL"
cli.publish("alarm", slogan, severity)
```

The component `monitor` subscribes to the `alarm` topic:

```python
import rembus as rb

def alarm(slogan, severity):
    print(f"{slogan}: {descr}")

sub = rb.node("monitor", msgfrom=rb.LastReceived)
sub.subscribe(alarm)
```

The subscribed function gets called with the using the message payload:
`slogan` as first argument and `severity` as second argument.

The option `msgfrom` is for receiving published messages when the component
was not subscribed to the topic, for example because it was not connected when
the messages was published.


### Subscribe to a topic space

You can subscribe to set of topics (a topic space) using a 'regular expression':
then all topics matching the pattern are subscribed.

The subscribed function gets called with the originating topic as
first argument followed by the pubsub message payload arguments. 

```python
import rembus as rb

def telemetry(topic, data):
    print(f"from {topic}: do something with {data}")

sub = rb.node("collector")
sub.subscribe(telemetry, topic="*/telemetry")
```

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

