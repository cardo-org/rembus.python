import pytest
import rembus as rb
from rembus.keyspace import build_space_re
from rembus.protocol import valid_pattern


@pytest.mark.parametrize(
    "pattern,topic,expected",
    [
        ("a/*", "a/b", True),
        ("a/*", "a/", False),
        ("a/*", "a/b/c", False),
        ("a/**", "a/", True),
        ("a/**", "a/b/c", True),
        ("a/**", "a", False),
        ("*", "foo", True),
        ("*", "a/b", False),
        ("**", "", True),
        ("a.b/*", "aXb/c", False),
        ("a/@x/*", "a/@x/b", True),
    ],
)
def test_pattern_matching(pattern, topic, expected):
    assert bool(build_space_re(pattern).match(topic)) is expected


@pytest.mark.parametrize(
    "pattern,valid",
    [("a/*", True), ("**", True), ("a*", False), ("a**b", False), ("a/*b/c", False)],
)
def test_valid_pattern(pattern, valid):
    assert valid_pattern(pattern) is valid


def test_subscribe_invalid_pattern(server):
    cli = rb.node("wild_invalid")
    with pytest.raises(rb.RembusError):
        cli.subscribe(lambda *a: None, topic="a*")
    cli.close()


def test_wildcard_rpc_and_expose_rejected(server):
    cli = rb.node("wild_rpc")
    with pytest.raises(rb.RembusError):
        cli.rpc("a/*")
    with pytest.raises(rb.RembusError):
        cli.expose(lambda: 1, topic="a/*")
    cli.close()


def test_wildcard_subscribe_without_keyspace():
    broker = rb.node(name="wild_nokeyspace", port=8339, keyspace=False)
    try:
        cli = rb.node("ws://127.0.0.1:8339/wild_nokeyspace_cli")
        with pytest.raises(rb.RembusError):
            cli.subscribe(lambda *a: None, topic="a/*")
        cli.close()
    finally:
        broker.close()
