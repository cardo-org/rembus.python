import rembus as rb


def mytopic():
    return 1


def another_topic():
    return 1


def save_exposers():
    bro = rb.node()

    srv = rb.node("srv")
    srv.expose(mytopic)

    srv.close()
    bro.close()


def test_exposers():
    save_exposers()

    bro = rb.node()
    srv = rb.node("srv")

    assert "mytopic" in bro.router.exposers

    # delete and insert
    srv.unexpose(mytopic)
    srv.expose(another_topic)

    srv.close()
    bro.close()


def test_final_cfg():
    bro = rb.node()
    srv = rb.node("srv")
    assert "mytopic" not in bro.router.exposers
    assert "another_topic" in bro.router.exposers

    srv.close()
    bro.close()
