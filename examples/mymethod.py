import rembus.sync as rb
import logging
import time

logging.basicConfig(encoding="utf-8", level=logging.DEBUG)

c = rb.process("myserver")

@rb.expose(c)
def foo_impl(n, name):
    return f"{n} hello world"

#time.sleep(2)
#c.close()
#time.sleep(2)
# c.publish("pppp", 10)
