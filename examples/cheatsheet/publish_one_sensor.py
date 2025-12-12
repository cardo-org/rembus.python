import polars as pl
import rembus as rb

cli = rb.node("ws://:9000/myclient")
cli.publish("mysite/HVAC/mysite.sensor3/sensor")

cli.close()
