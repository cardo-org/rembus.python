import rembus


def add(x, y):
    return x+y


rb = rembus.node('calculator')
rb.expose(add)
rb.wait()
