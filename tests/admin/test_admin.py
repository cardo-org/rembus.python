import logging
import pytest
from tools.admin import add_admin
import rembus as rb

def test_add_admin():
    dbpath = rb.settings.db_attach("broker")

    logging.info("[add_admin] db: %s", dbpath)
    name = "admin"
    add_admin(name)

    bro = rb.node()

    node = rb.node(name)
    node.private_topic("private_topic")

    notauth = rb.node("laqualunque")
    with pytest.raises(rb.RembusError):
        notauth.private_topic("some_topic")

    node.close()
    notauth.close()
    bro.close()