import rembus as rb


def test_setup_install():
    foo_code = """
def foo(x,y):
    return x+y
"""
    srv = rb.node(name="orchestrator")

    with rb.node("distributor") as n:
        n.rpc("python_service_install", "myservice", foo_code)

    srv.close()


def test_restart():
    srv = rb.node(name="orchestrator")

    with rb.node("distributor") as n:
        result = n.rpc("myservice", 1, 1)
        assert result == 2

    srv.close()
