import rembus as rb


def sensor(topic):
    print(f"from {topic}: do something with this sensor")


sub = rb.node("device_inventory")
sub.subscribe(sensor, topic="**/sensor")
sub.reactive()
sub.register_shutdown()
sub.wait()
