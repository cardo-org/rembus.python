# Rembus for Python

[![Docs Stable](https://img.shields.io/badge/docs-stable-blue)](https://cardo-org.github.io/rembus.python/stable/)
[![Docs Dev](https://img.shields.io/badge/docs-dev-orange)](https://cardo-org.github.io/rembus.python/dev/)
[![Build Status](https://github.com/cardo-org/rembus.python/actions/workflows/python-app.yml/badge.svg?branch=main)](https://github.com/cardo-org/rembus.python/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/cardo-org/rembus.python/branch/main/graph/badge.svg)](https://codecov.io/gh/cardo-org/rembus.python)

Rembus is a Pub/Sub and RPC middleware.

## Features

* Binary message encoding using [CBOR](https://cbor.io/).

* Native support for exchanging DataFrames.

* Persistent storage via [DuckDB DuckLake](https://ducklake.select/).

* Pub/Sub QOS0, QOS1 and QOS2.

* Hierarchical topic routing with wildcards (`*/*/temperature`).

* MQTT integration.

* WebSocket transport.

See [Rembus.jl](https://cardo-org.github.io/Rembus.python/stable/) broker
for a full fledged broker that supports WebSocket, ZMQ and plain tcp protocols
and more features like private topics, multi-tenancy and more.

## Concepts

* **Component**: an addressable node in a distributed system. A component
connects to a broker and communicates using Pub/Sub and/or RPC semantics.

* **Broker**: a specialized component responsible for routing Pub/Sub messages
and dispatching RPC calls between components.
A broker may also persist messages and expose services.

* **Topic**: a "logical channel" string identifier used for Pub/Sub message
routing (e.g. `alarm_topic`).

* **Topic space**: a set of topics defined by wildcard patterns
(e.g. `*/telemetry`) used for bulk subscription..

* **Subscription**: a callback bound to a topic or topic space; invoked
automatically with the message payload when a Pub/Sub message is published.
Supports wildcard topics and optional delivery of messages sent while the
subscriber was offline (`msgfrom=rb.LastReceived`).

* **RPC Service**: a named function exposed by a component and registered at
the broker for remote invocation.

* **RPC Call**: a synchronous or asynchronous request issued by a component
to invoke a remote RPC service.

* **Schema**: an optional declarative mapping between topic patterns and
persistent storage tables, used to structure and persist messages at rest.

* **Data at Rest**: broker capability to persist published messages into DuckDB
tables defined by a schema, enabling historical queries and analytics.

## Getting Started

Install the package:

```shell
pip install rembus
```

### Broker

Start a broker (sync or async):

```python
import rembus as rb

# sync version
bro = rb.node() # equivalent to rb.node(port = 8000)
bro.wait() # event loop, not needed in interactive interpreter


# async version
bro = await rb.component()
await bro.wait() 
```

### Broker with persistent storage

A Rembus broker can be configured with a **schema** to persist published
messages into a [DuckDB](https://duckdb.org/) database via the
[DataLake](https://ducklake.select/) extension.

A schema declaratively maps **Pub/Sub topic patterns** to **relational tables**.
Topics variables are extracted from the topic path and mapped to table columns;
message payload fields are mapped to the remaining columns.

Example `schema.json`:

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
            "extras": {
                "recv_ts": "ts",
                "slot": "time_bucket"
            }
        }
    ]
}
```

### Schema semantics

* Each entry in `tables` defines a **topic-to-table binding**.

* Topic segments prefixed with `:` are variables extracted from the topic
path and written to the corresponding columns.

* Message payload fields are mapped positionally or by name to remaining columns.

* `keys` define logical primary keys.

* `extras` specify broker-generated metadata (e.g. receive timestamp, time
bucketing).

### Example mappings

* Messages published to `:site/:type/:dn/sensor` are persisted in the `sensor`
table, with `site`, `type`, and `dn` derived from the topic path.

* Messages published to `:dn/telemetry` are persisted in the `telemetry` table,
with metric values extracted from the message payload.

If a schema is provided, DuckLake tables are created automatically if they
do not already exist, and all matching publications are persisted.

For each table two special topics are automatically created:

* `query_<table>` retrieve records;
* `delete_<table>` remove records;

RPC calls to `query_*` topics return a Polars DataFrame.

For example:

```python
cli.rpc("query_sensor", {"where": "type='HVAC'"})
```

### Starting a broker with storage enabled

```python
import rembus as rb

bro = await rb.component(schema="schema.json")
await bro.wait() 
```

### Components

Connect to a Broker:

```python
# named component
cli = await rb.component("ws://host:8000/myname")

# default host/port
cli = await rb.component("myname")

# anonymous
cli = await rb.component(rb.anonym(host="host", port=8000))
```

### Why named components

* Enables authentication (RSA/ECDSA/shared secret).

* Allows persistent twin mapping; offline messages are buffered.
A Component connects to a Broker using a URL-like formatted string that
identifies the broker address and declare the name of the Component.

## Pub/Sub

### Publish

```python
# Single message
await cli.publish("site/type/dn/sensor")
await cli.publish("dn/telemetry", {'temperature': 21.6, 'pressure': 980})

# DataFrame
import polars as pl
df = pl.DataFrame({"dn":["s1","s2"], "temperature":[15,18.8]})
await cli.publish("telemetry", df)
```

### Subscribe

```python
# Single topic
def alarm(slogan, severity):
    print(f"{slogan}: {severity}")

sub = rb.node("monitor")
sub.subscribe(alarm, topic="alarm") # equivalent to sub.subscribe(alarm)

# Topic space
def telemetry(topic, data):
    print(f"{topic}: {data}")

sub.subscribe(telemetry, topic="**/telemetry", msgfrom=rb.LastReceived)
```

The subscribed `alarm` function gets called with the using the message payload:
`slogan` as first argument and `severity` as second argument:

```python
cli.publish("alarm", "mydevice: battery very low", "CRITICAL")
```

The option `msgfrom` is for receiving published messages when the component
was not subscribed to the topic, for example because it was not connected when
the messages was published.

## RPC

### Expose

```python
import rembus as rb

def add(x,y):
    return x+y

handle = rb.node('calculator')
handle.expose(add)
handle.wait()
```

### Call

```python
import rembus as rb

handle = rb.node("myclient")
result = handle.rpc('add', 1, 2)
```
