import logging
import rembus
import rembus.protocol as rp
import time

def test_register():
    server = rembus.node(port=8000)
    rb = rembus.register("test_register", "11223344")
    server.close()

