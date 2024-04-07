# Rembus API for Python

Rembus is a Pub/Sub and RPC middleware.

There are few key concepts to get confident with Rembus:

- A Component is a distributed application that communicate with Pub/Sub or RPC styles;
- A Component connect to a Broker;
- A Broker dispatch messages between Components;
- A Component expose RPC services and/or subscribe to Pub/Sub topics;
- A Component make RPC requests and/or publish messages to Pub/Sub topics;

This API version supports only the WebSocket protocol.

## Getting Started

Start a Rembus broker, for example [caronte](https://cardo-org.github.io/Rembus.jl/stable/).

Install the package:

```shell
pip install rembus
```

then import `rembus.sync` for using the synchronous Python API:  

```python
import rembus.sync as rembus

rb = rembus.component()
rb.publish({'name': 'sensor_1', 'metric': 'T', 'value':21.6})
rb.close()
```

or import `rembus` for using the asynchronous Python API:

```python
import asyncio
import rembus

async def main():
    rb = await rembus.component(client_name)

    await rb.publish("mytopic", 
                         {
                            'name': 'sensor_1',
                            'metric': 'T',
                            'value':21.6
                         })

    await rb.close()


loop = asyncio.new_event_loop()
loop.run_until_complete(main())

```

## Initialize a Component

Currently the Python API provides the WebSocket protocol for connecting to the Rembus broker.

The url argument of the `component` function define the component identity and the broker endpoint to connect:

```python
import rembus

# Broker endpoint and named component
rb = rembus.component('ws://hostname:port/component_name')

# Broker endpoint and anonymous component 
rb = rembus.component('ws://hostname:port')

# Default broker and named component 
rb = rembus.component('component_name')

# Default broker and anonymous component 
rb = rembus.component()
```

The `component` builder function returns a Rembus handler that will be used for interacting with the components via Pub/Sub and RPC messages.

`component_name` is the unique name that assert the component identity between online sessions (connect/disconnect windows).

`component_name` is optional: if it is missing then a random identifier that changes at each connection event is used as the component identifier. In this case the broker is unable to bind the component to a persistent twin and messages published when the component is offline get not broadcasted to the component when it gets online again.

The default broker endpoint is set by `REMBUS_BASE_URL` environment variable and default to `ws://127.0.0.1:8000`.

## Pub/Sub example

A message is published with `publish` function.

```python
rb.publish('mytopic', arg_1, arg_2, ..., arg_N)
```

Where the arguments `arg_i` comprise the message data payload that gets received by the subscribed components.

A subscribed component interested to the topic `mytopic` have to define a function named as the topic of interest and with the same numbers of arguments:


```python
# do something each time a message published to topic mytopic is published
def mytopic(arg_1, arg_2, ..., arg_N):
    ...

rb.subscribe(mytopic)

rb.forever()
```

The first argument to `subscribe` is the function, named as the topic of interest, that will be called each time a message is published.

The optional second argument of `subscribe` define the "retroactive" feature of the
subscribed topic.

If the second argument is `True` then the messages published when the component is offline will be delivered as soon as the component will get online again, otherwise
the messages published before connecting will be lost. 

> **NOTE**: To cache messages for an offline component the broker needs to know that such component has subscribed for a specific topic. This imply that messages published before the first subscribe happens will be lost. If you want all message will be delivered subscribe first and publish after.  

## RPC example

A RPC service is implemented with a function named as the exposed service.

```python
import rembus.sync as rembus

def add(x,y):
    return x+y

rb = rembus.component('calculator')

rb.expose(add)

rb.forever()
```

The `calculator` component expose the `add` service, the RPC client will invoke as:

```python
import rembus.sync as rembus

rb = rembus.component()
result = rb.rpc('add', 1, 2)
```

The asynchronous client and server implementations will be something like:

```python
#server.py
import asyncio
import rembus

async def add(x, y):
    return x+y

async def main():
    rb = await rembus.component()
    
    await rb.expose(add)
    await rb.forever()

loop = asyncio.new_event_loop()
loop.run_until_complete(main())
```

```python
# client.py
import asyncio
import rembus

async def main():
    rb = await rembus.component()
    result = await rb.rpc('add', 1, 2)
    print(f'result={result}')
    await rb.close()


loop = asyncio.new_event_loop()
loop.run_until_complete(main())
```
