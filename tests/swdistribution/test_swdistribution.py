import pytest
import rembus as rb


def test_service_install(server):
    foo_code = """
def foo(x,y):
    return x+y
"""

    with rb.node("distributor") as n:
        n.rpc(
            "python_service_install", {"name": "myservice", "content": foo_code}
        )


def test_invoke_myservice(server):
    x = 1
    y = 3
    with rb.node("distributor") as n:
        result = n.rpc("myservice", x, y)
        assert result == x + y


def test_service_uninstall(server):
    with rb.node("distributor") as n:
        n.rpc("python_service_uninstall", "myservice")


def test_try_invoke_myservice(server):
    x = 1
    y = 3
    with rb.node("distributor") as n:
        with pytest.raises(rb.RembusError):
            n.rpc("myservice", x, y)


def test_subscribers_install(server):
    code = """
def bar(x,y):
    print(f"bar topic invoked: x={x}, y={y}")
"""

    with rb.node("distributor") as n:
        n.rpc("python_subscriber_install", {"name": "mytopic", "content": code})


def test_publish_mytopic(server):
    x = 1
    y = 3
    with rb.node("distributor") as n:
        n.publish("mytopic", x, y)


def test_subscribers_uninstall(server):
    with rb.node("distributor") as n:
        n.rpc("python_subscriber_uninstall", "mytopic")


def test_service_install_on_target(server):
    foo_code = """
def foo(x,y):
    return x*y
"""
    target_name = "srv"
    target_node = rb.node(target_name)

    with rb.node("distributor") as n:
        n.direct(
            target_name,
            "python_service_install",
            {"name": "myservice", "content": foo_code},
        )

    x = 2
    y = 4
    with rb.node("distributor") as n:
        result = n.rpc("myservice", x, y)
        assert result == x * y

    target_node.close()
