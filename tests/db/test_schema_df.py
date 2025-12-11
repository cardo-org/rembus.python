import asyncio
import polars as pl
import rembus as rb
from tests.db.broker import start_broker


async def test_init_db():
    bro = await start_broker("schema_df.json")
    await asyncio.sleep(1)

    pub = await rb.component("test_pub")

    topic1_df = pl.DataFrame(
        {
            "name": ["name_a", "name_b", "name_c"],
            "type": ["type_a", "type_b", "type_c"],
            "tinyint": [1, 2, 3],
            "smallint": [11, 22, 33],
            "integer": [16, 32, 48],
            "bigint": [64, 128, 256],
        }
    )

    invalid_df = pl.DataFrame(
        {
            "name": ["name_a", "name_b", "name_c"],
            "type": ["type_a", "type_b", "type_c"],
        }
    )

    topic2_df = pl.DataFrame(
        {
            "name": ["name_a", "name_b", "name_c"],
            "type": ["type_a", "type_b", "type_c"],
            "utinyint": [1, 2, 3],
            "usmallint": [11, 22, 33],
            "uinteger": [16, 32, 48],
            "ubigint": [64, 128, 256],
        },
        schema={
            "name": pl.Utf8,
            "type": pl.Utf8,
            "utinyint": pl.UInt8,
            "usmallint": pl.UInt16,
            "uinteger": pl.UInt32,
            "ubigint": pl.UInt64,
        },
    )

    topic_without_extras_df = pl.DataFrame(
        {
            "name": ["name_a", "name_b", "name_c"],
            "type": ["type_a", "type_b", "type_c"],
            "value": ["a", "b", "c"],
        },
        schema={
            "name": pl.Utf8,
            "type": pl.Utf8,
            "value": pl.Utf8,
        },
    )

    await pub.publish("topic1", topic1_df)
    await pub.publish("topic1", topic1_df)
    await pub.publish("topic1", invalid_df)

    # topic2 is of type upsert
    await pub.publish("topic2", topic2_df)
    await pub.publish("topic2", topic2_df)
    await pub.publish("topic2", invalid_df)

    # this is not saved because a dataframe is expected
    await pub.publish("topic_without_extras", "a string is not a dataframe")

    await pub.publish(
        "topic_without_extras",
        pl.DataFrame(
            schema={
                "name": pl.String,
                "type": pl.String,
                "value": pl.String,
            }
        ),
    )
    await asyncio.sleep(2)

    await pub.publish("topic_without_extras", topic_without_extras_df)

    await asyncio.sleep(3)

    await pub.close()

    db = bro.db
    topic1_df = db.execute("select * from topic1").pl()
    assert topic1_df.shape[0] == 6

    topic2_df = db.execute("select * from topic2").pl()
    assert topic2_df.shape[0] == 3

    await bro.close()
