import pytest
import rembus as rb


def foo():
    pass


@pytest.mark.parametrize(
    "topic,msg_from",
    [
        ("foo", -1),
        ("foo", float("nan")),
        ("foo", "abc"),
        ("", 0),
        ("__reserved__", 0),
    ],
)
def test_subscribe_invalid(server, topic, msg_from):
    cli = rb.node("sub_invalid")
    with pytest.raises(rb.RembusError):
        cli.subscribe(foo, msg_from, topic=topic)
    cli.close()


def test_subscribe_last_received(server):
    cli = rb.node("sub_valid")
    cli.subscribe(foo, rb.LastReceived)
    cli.close()
