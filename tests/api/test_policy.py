"""Test the rembus broker feature."""

import logging
import rembus
import rembus.protocol as rp


def myservice1(x, y):
    return x + y


def myservice2(x, y):
    return x * y


def test_round_robin():
    """Test a broker with round_robin policy."""
    x = 1
    y = 2
    server = rembus.node(port=8801, policy="round_robin")
    srv1 = rembus.node("ws://:8801/srv1")
    srv1.expose(myservice1, topic="myservice")
    srv2 = rembus.node("ws://:8801/srv2")
    srv2.expose(myservice2, topic="myservice")

    cli = rembus.node("ws://:8801/cli")

    try:
        z = cli.rpc("invalid_service", x, y)
    except rp.RembusError as e:
        logging.info("caught expected exception: %s", e)
        assert True, "method is unknown"

    z = cli.rpc("myservice", x, y)
    assert z == x + y

    z = cli.rpc("myservice", x, y)
    assert z == x * y

    z = cli.rpc("myservice", x, y)
    assert z == x + y

    srv2.close()

    z = cli.rpc("myservice", x, y)
    assert z == x + y

    srv2 = rembus.node("ws://:8801/srv2")
    srv2.expose(myservice2, topic="myservice")

    srv1.unexpose("myservice")
    srv2.unexpose("myservice")

    try:
        z = cli.rpc("myservice", x, y)
        assert False, "unexpected method result"
    except Exception as e:
        logging.info("caught expected exception: %s", e)
        assert True, "method has not exposers"

    srv1.close()
    srv2.close()
    # Test that closing a closed twin does not raise.
    srv2.close()
    cli.close()
    server.close()


def test_less_busy():
    """Test a broker with round_robin policy."""
    x = 1
    y = 2
    server = rembus.node(port=8801, policy="less_busy")
    srv1 = rembus.node("ws://:8801/srv1")
    srv1.expose(myservice1, topic="myservice")
    srv2 = rembus.node("ws://:8801/srv2")
    srv2.expose(myservice2, topic="myservice")

    cli = rembus.node("ws://:8801/cli")

    z = cli.rpc("myservice", x, y)
    assert z == x + y

    z = cli.rpc("myservice", x, y)
    assert z == x + y

    srv2.close()

    z = cli.rpc("myservice", x, y)
    assert z == x + y

    srv2 = rembus.node("ws://:8801/srv2")
    srv2.expose(myservice2, topic="myservice")

    srv1.unexpose("myservice")
    srv2.unexpose("myservice")

    try:
        z = cli.rpc("myservice", x, y)
        assert False, "unexpected method result"
    except Exception as e:
        logging.info("caught expected exception: %s", e)
        assert True, "method has not exposers"

    srv1.close()
    srv2.close()
    cli.close()
    server.close()


def test_wrong_policy():
    """Test invalid policy"""
    try:
        rembus.node(port=8801, policy="wrong_policy")
    except ValueError as e:
        logging.info("caught expected exception: %s", e)
        assert True, "invalid policy"
