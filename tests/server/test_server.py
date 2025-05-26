import rembus
import rembus.protocol as rp
import shutil
import time

import rembus.settings

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

def test_named():
    server = rembus.node(port=8010)
    rb1 = rembus.node("ws://:8010/test_mycomponent")
    rb2 = rembus.node("ws://:8010/test_another")
    
    rid = rb1.rpc("rid")
    assert rid == rembus.settings.DEFAULT_BROKER
    
    server.close()
    rb1.close()
    rb2.close()
