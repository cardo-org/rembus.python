import polars as pl
import rembus as rb

df = pl.DataFrame(
    {
        "site": ["mysite", "mysite"],
        "dn": ["mysite.sensor1", "mysite.sensor2"],
        "type": ["HVAC", "HVAC"],
    }
)

cli = rb.node("ws://:9000/myclient")
cli.publish("sensor", df)
cli.close()
