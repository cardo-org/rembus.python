import logging
import time
import pytest
from tools.admin import add_admin
import rembus as rb

ADMIN_NODE = "admin"


def mymethod(ctx, node):
    return "secret"


def mytopic(originating_name, ctx, node):
    logging.debug("received mytopic message")
    ctx[originating_name] = True


def test_add_admin():
    dbpath = rb.settings.db_attach("broker")

    logging.info("[add_admin] db: %s", dbpath)
    add_admin(ADMIN_NODE)

    bro = rb.node()

    node = rb.node(ADMIN_NODE)
    node.private_topic("private_topic")

    notauth = rb.node("laqualunque")
    with pytest.raises(rb.RembusError):
        notauth.private_topic("some_topic")

    node.close()
    notauth.close()
    bro.close()


def test_authorize():
    auth_client_name = "mynode"
    unauth_client_name = "yournode"
    srv_name = "srv"
    bro = rb.node()

    ctx = {}
    srv = rb.node(srv_name)
    srv.expose(mymethod)
    srv.subscribe(mytopic)
    srv.inject(ctx)
    srv.reactive()

    node = rb.node(ADMIN_NODE)

    node.authorize(auth_client_name, "mymethod")

    # authorize to publish
    node.authorize(auth_client_name, "mytopic")

    # authorize to subscribe
    node.authorize(srv_name, "mytopic")

    auth_client = rb.node(auth_client_name)
    unauth_client = rb.node(unauth_client_name)

    # not authorized to change settings
    with pytest.raises(rb.RembusError):
        unauth_client.private_topic("some_topic")

    with pytest.raises(rb.RembusError):
        unauth_client.public_topic("some_topic")

    with pytest.raises(rb.RembusError):
        unauth_client.authorize(unauth_client_name, "some_topic")

    with pytest.raises(rb.RembusError):
        unauth_client.unauthorize(unauth_client_name, "some_topic")

    value = auth_client.rpc("mymethod")
    assert value == "secret"

    with pytest.raises(rb.RembusError):
        unauth_client.rpc("mymethod")

    auth_client.publish("mytopic", auth_client_name)
    unauth_client.publish("mytopic", unauth_client_name)

    time.sleep(1)
    assert auth_client_name in ctx
    assert unauth_client_name not in ctx

    node.unauthorize(srv_name, "mymethod")

    # Revert mytopic to public
    node.public_topic("mytopic")
    unauth_client.publish("mytopic", unauth_client_name)
    time.sleep(1)
    assert unauth_client_name in ctx

    auth_client.close()
    unauth_client.close()
    srv.close()
    node.close()
    bro.close()
