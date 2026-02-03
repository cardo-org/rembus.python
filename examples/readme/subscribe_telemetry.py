import rembus as rb


def telemetry(topic, data):
    print(f"from {topic}: do something with {data}")


sub = rb.node("collector")
sub.subscribe(telemetry, topic="*/telemetry")
sub.reactive()
sub.wait()
