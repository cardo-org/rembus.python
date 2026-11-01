import pytest
import rembus as rb


@pytest.mark.parametrize("topic", ["", "__reserved__"])
def test_rpc_invalid_topic(server, topic):
    cli = rb.node("rpc_invalid_topic")
    with pytest.raises(rb.RembusError):
        cli.rpc(topic)
    cli.close()
