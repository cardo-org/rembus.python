import asyncio
import os
import numpy as np
import rembus as rb
from tests.db.broker import start_broker


async def test_init_db():
    temperature = 99.0

    bro = await start_broker("hierarchy.json")
    await asyncio.sleep(1)

    pub = await rb.component("test_pub")
    await pub.publish(
        "veneto/agordo/temperature",
        {
            "value": temperature,
            "sensor": "sensor_xyz",
        },
    )

    await pub.publish(
        "veneto/agordo/not_exists",
        {
            "value": temperature,
            "sensor": "sensor_xyz",
        },
    )

    # The topic does not match the hierarchy, so it is not saved into any
    # specific table.
    await pub.publish(
        "veneto/temperature",
        {
            "value": temperature,
            "sensor": "sensor_abc",
        },
    )

    await pub.publish("mysite/type_1/myname/device")

    await asyncio.sleep(3)

    db = bro.db
    df = db.execute("SELECT * from device").pl()
    assert df.shape[0] == 1

    await pub.close()
    await bro.close()
