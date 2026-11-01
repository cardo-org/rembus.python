import pytest
import rembus as rb


def foo():
    pass


@pytest.mark.parametrize("topic", ["", "__reserved__"])
def test_unsubscribe_invalid_topic(server, topic):
    cli = rb.node("unsub_invalid")
    with pytest.raises(rb.RembusError):
        cli.unsubscribe(topic)
    cli.close()


def test_unsubscribe_idempotent(server):
    cli = rb.node("unsub_idem")
    cli.unsubscribe("never_subscribed")
    cli.subscribe(foo)
    cli.unsubscribe(foo)
    cli.unsubscribe(foo)
    cli.close()
