import os
import rembus


async def start_broker(schema_file):
    schema_fn = os.path.join(os.path.dirname(__file__), schema_file)
    bro = await rembus.component(schema=schema_fn)
    db = bro.db
    assert db is not None

    for table_name in bro.router.tables:
        db.execute(f"DELETE FROM {table_name}")

    return bro
