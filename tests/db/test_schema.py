import asyncio
import logging
import rembus as rb
import pytest
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

    await pub.publish("topic2", "name", "wrong_number_of_fields")

    await pub.publish("topic3", {"name": "name_a", "double": 3.0})
    await pub.publish(
        "topic4", {"name": "name_a", "type": "type_a", "value": "value_a"}
    )

    try:
        await pub.publish(
            "topic3",
            "name_a",
            -1.0,
        )
    except Exception as e:
        print("Expected exception:", e)

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
    assert topic3_df.shape[0] == 1

    await pub.rpc(
        "deletelake",
        {
            "table": "topic3",
            "where": {"name": "name_a", "type": "type_default"},
        },
    )
    topic3_df = db.execute("select * from topic3").pl()
    assert topic3_df.shape[0] == 0

    # test errors
    with pytest.raises(rb.RembusError):
        await pub.rpc(
            "deletelake",
            {
                "where": {"name": "name_a", "type": "type_default"},
            },
        )

    with pytest.raises(rb.RembusError):
        await pub.rpc(
            "deletelake",
            {
                "table": "topic3",
            },
        )

    await pub.close()
    await sub.close()
    await bro.close()
