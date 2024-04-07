import rembus.sync as rembus

def add(x,y):
    return x+y

rb = rembus.component('calculator')

rb.expose(add)

rb.forever()