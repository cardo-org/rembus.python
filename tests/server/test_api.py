import asyncio
import logging
import pytest
import rembus
import rembus.protocol as rp
import time

import rembus.settings

async def shutdown(rb):
    await asyncio.sleep(0.1)
    await rb.close()

def start_server(port):
    server = rembus.node(port=port)
    time.sleep(1)
    return server

async def test_wait():
    server = start_server(port=8001)
    rb = await rembus.component("ws://:8001")

    rid = await rb.rpc("rid")
    assert rid == "broker"

    # schedule the shutdown
    asyncio.create_task(shutdown(rb))

    await rb.wait()
    await rb.close()
    server.close()

async def myservice(x,y):
    return x+y

async def test_rpc():
    x = 2
    y = 3
    server = start_server(port=8002)
    rb = await rembus.component("ws://:8002")
    server.expose(myservice)

    result = await rb.rpc("myservice", x, y)
    assert result == x+y

    server.unexpose(myservice)

    await rb.close()

    with pytest.raises(rp.RembusConnectionClosed):
        await rb.rpc("myservice", x, y) 

    server.close()

async def myservice_ctx(ctx, rb, x,y):
    return x+y

async def test_rpc_ctx():
    x = 2
    y = 3
    server = start_server(port=8003)
    
    ctx = {}
    rb = await rembus.component("ws://:8003")
    server.expose(myservice_ctx)
    server.inject(ctx)

    result = await rb.rpc("myservice_ctx", x, y)
    assert result == x+y

    await rb.close()

    server.close()

async def test_direct():
    x = 2
    y = 3
    server = start_server(port=8004)
    
    rb = await rembus.component("ws://:8004")
    server.expose(myservice)

    result = await rb.direct(rembus.settings.DEFAULT_BROKER, "myservice", x, y)
    assert result == x+y

    server.unexpose(myservice)

    await rb.close()
    server.close()

async def test_unreactive():
    server = start_server(port=8005)
    
    rb = await rembus.component("ws://:8005")
    await rb.unreactive() 
    await rb.close()
    server.close()

def mytopic():
    logging.info(f"mytopic called")

async def test_publish():
    server = start_server(port=8006)
    server.subscribe(mytopic)

    rb = await rembus.component("ws://:8006/cmp.net")
    assert rb.isrepl() == False
    assert isinstance(rb.router, rembus.twin.Router)
    assert server.router.__repr__() == "broker: {'cmp.net': cmp.net}"
    assert rb.uid.__repr__() == "ws://127.0.0.1:8006/cmp.net"
    assert rembus.twin.domain(rb.rid) == "net"
    
    await rb.publish("mytopic")
    
    await rb.publish("mytopic", "log_warning")
    
    await rb.close()

    with pytest.raises(rp.RembusConnectionClosed):
        await rb.publish("mytopic") 

    server.close()

