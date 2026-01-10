import asyncio
from datetime import datetime, timedelta, timezone
import logging
import pytest
import rembus as rb
from tests.db.broker import start_broker


def topic4(d):
    logging.info("[topic4] recv: %s", d)


async def test_init_db():
    bro = await start_broker("schema.json")
    await asyncio.sleep(0.1)

    sub = await rb.component("sub")
    await sub.subscribe(topic4)
    await sub.reactive()

    pub = await rb.component("test_pub")
    await pub.publish(
        "topic1", "name_a", "type_a", 1, 16, 32, 64, slot=1234, qos=rb.QOS2
    )

    await pub.publish(
        "topic2",
        "name_a",
        "type_a",
        1,
        16,
        32,
        64,
    )

    # now_dt = datetime.now(timezone.utc)
    now_dt = datetime.now()

    await pub.publish("topic2", "name", "wrong_number_of_fields")
    await pub.publish("topic3", {"name": "name_a", "double": 3.0})
    await asyncio.sleep(2)

    await pub.publish("topic3", {"name": "name_a", "double": 4.0})
    await pub.publish(
        "topic4", {"name": "name_a", "type": "type_a", "value": "value_a"}
    )

    await pub.publish(
        "topic3",
        "name_a",
        -1.0,
    )

    # this messages is not saved becaus it miss mandatory fields defined in
    # the schema.
    await pub.publish("topic1", "foo")
    await pub.publish("topic4", {"name": "bar"})
    await pub.publish(
        "topic3",
        {
            "field_not_in_schema": "unknown_field",
            "unknown_field": "wrong field",
        },
    )

    await asyncio.sleep(3)

    db = bro.db

    topic1_df = db.execute("select * from topic1").pl()
    assert topic1_df.shape[0] == 1
    logging.info(topic1_df)

    topic3_df = db.execute("select * from topic3").pl()
    assert topic3_df.shape[0] == 2

    df = await pub.rpc("query_topic3")
    assert df.shape[0] == 2

    timepoint = now_dt + timedelta(seconds=2)
    df = await pub.rpc(
        "query_topic3",
        {"when": timepoint.strftime(
            "%Y-%m-%d %H:%M:%S"), "where": "double=3.0"},
    )
    assert df.shape[0] == 1

    # Using epoch seconds
    df = await pub.rpc(
        "query_topic3",
        {"when": timepoint.timestamp(), "where": "double=3.0"},
    )
    assert df.shape[0] == 1

    df = await pub.rpc("query_topic3", {"where": "double>10"})
    assert df.shape[0] == 0

    await pub.rpc(
        "delete_topic3",
        {
            "where": "name='name_a' and type='type_default'"
        },
    )
    df = db.execute("select * from topic3").pl()
    assert df.shape[0] == 0

    # no where condition
    await pub.rpc("delete_topic3")

    # test errors
    with pytest.raises(rb.RembusError):
        await pub.rpc(
            "query_topic3",
            {
                "no_where": {"name": "name_a", "type": "type_default"},
            },
        )

    with pytest.raises(rb.RembusError):
        await pub.rpc(
            "delete_topic3",
            {
                "no_where": {"name": "name_a", "type": "type_default"},
            },
        )

    await pub.close()
    await sub.close()
    await bro.close()
