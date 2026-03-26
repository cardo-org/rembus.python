import logging
import os
import pytest
import polars as pl
import rembus as rb


def test_upsert_invalid_record():
    fn = os.path.join(os.path.dirname(__file__), "schema.json")
    bro = rb.node(schema=fn)
    cli = rb.node("cli")

    with pytest.raises(rb.RembusError):
        cli.rpc("upsert_topic1", 1)

    cli.close()
    bro.close()


def test_upsert_topic1():
    fn = os.path.join(os.path.dirname(__file__), "schema.json")
    bro = rb.node(schema=fn)
    cli = rb.node("cli")

    obj = {
        "name": "myname",
        "type": "router",
        "tinyint": 100
    }
    res = cli.rpc("upsert_topic1", obj)
    assert res is None

    df = cli.rpc("query_topic1")
    assert df.shape[0] == 1
    assert df[0, "name"] == obj["name"]
    assert df[0, "type"] == obj["type"]
    assert df[0, "tinyint"] == obj["tinyint"]

    cli.close()
    bro.close()


def test_upsert_topic1_list():
    fn = os.path.join(os.path.dirname(__file__), "schema.json")
    bro = rb.node(schema=fn)
    cli = rb.node("cli")

    cli.rpc("delete_topic1")

    obj = [
        {
            "name": "myname_1",
            "type": "router",
            "tinyint": 1
        }, {
            "name": "myname_2",
            "type": "router",
            "tinyint": 2
        }]

    res = cli.rpc("upsert_topic1", obj)
    assert res is None

    df = cli.rpc("query_topic1")
    assert df.shape[0] == 2
    assert df[0, "name"] == obj[0]["name"]
    assert df[1, "name"] == obj[1]["name"]

    cli.close()
    bro.close()


def test_upsert_topic1_df():
    fn = os.path.join(os.path.dirname(__file__), "schema.json")
    bro = rb.node(schema=fn)
    logging.info("db attach string: %s", bro.db_attach)
    cli = rb.node("cli")

    cli.rpc("delete_topic1")

    obj = pl.DataFrame([{
        "name": "myname1",
        "type": "router",
        "tinyint": 20
    },
        {
        "name": "myname2",
        "type": "router",
        "tinyint": 30
    }],
        schema={
        "name": pl.String,
        "type": pl.String,
        "tinyint": pl.Int8
    })

    res = cli.rpc("upsert_topic1", obj)
    assert res is None
    df = cli.rpc("query_topic1")
    expected = obj.sort("name")
    actual = df.select(["name", "type", "tinyint"]).sort("name")
    assert expected.equals(actual)
    cli.close()
    bro.close()


def test_upsert_topic2():
    fn = os.path.join(os.path.dirname(__file__), "schema.json")
    bro = rb.node(schema=fn)
    cli = rb.node("cli")

    cli.rpc("delete_topic2")

    obj = {
        "name": "myname",
        "type": "router",
        "utinyint": 1
    }
    cli.rpc("upsert_topic2", obj)

    obj["utinyint"] = 2
    cli.rpc("upsert_topic2", obj)

    df = cli.rpc("query_topic2")
    assert df.shape[0] == 1
    assert df[0, "name"] == obj["name"]
    assert df[0, "type"] == obj["type"]
    assert df[0, "utinyint"] == obj["utinyint"]

    cli.close()
    bro.close()


def test_upsert_topic2_df():
    fn = os.path.join(os.path.dirname(__file__), "schema.json")
    bro = rb.node(schema=fn)
    cli = rb.node("cli")

    cli.rpc("delete_topic2")

    obj = pl.DataFrame([{
        "name": "myname1",
        "type": "router",
        "utinyint": 20
    },
        {
        "name": "myname2",
        "type": "router",
        "utinyint": 30
    }],
        schema={
        "name": pl.String,
        "type": pl.String,
        "utinyint": pl.Int8
    })

    cli.rpc("upsert_topic2", obj)

    obj = obj.with_columns(pl.lit("switch").alias("type"))
    cli.rpc("upsert_topic2", obj)
    df = cli.rpc("query_topic2")
    expected = obj.sort("name")
    actual = df.select(["name", "type", "utinyint"]).sort("name")
    assert expected.equals(actual)

    cli.close()
    bro.close()
