import logging
import rembus
import rembus.protocol as rp
import time

def test_reconnect():
    server = rembus.node(port=8000)
    
    rb = rembus.node("ws:")

    rid = rb.rpc("rid")
    assert rid == "broker"
    server.close()
    
    time.sleep(3)

    server = rembus.node(port=8000)
    time.sleep(3)
    rid = rb.rpc("rid")
    assert rid == "broker"
   
    rb.close()
    server.close()

