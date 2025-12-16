import logging
import os
import time
import rembus as rb


def publish_something():
    fn = os.path.join(os.path.dirname(__file__), "schema.json")
    bro = rb.node(schema=fn)
    logging.info("db attach string: %s", bro.db_attach)
    cli = rb.node("cli")
    cli.publish("mysite/type_1/myname/device")
    time.sleep(1)
    cli.close()
    bro.close()


def test_create_sqlite():
    os.environ["DUCKLAKE_URL"] = "sqlite:tmp/rembus/rembus_test.sqlite"
    rb.db.reset_db("broker")
    publish_something()


def test_create_postgres():
    os.environ["DUCKLAKE_URL"] = (
        "postgres:postgresql://admin:secret@localhost/rembus_test"
    )
    rb.db.reset_db("broker")
    publish_something()


def test_create_duckdb():
    os.environ.pop("DUCKLAKE_URL")

    rb.db.reset_db("broker")
    publish_something()
