from datetime import datetime
from functools import partial
import os
import subprocess
from pathlib import Path
import logging
import shutil
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
import json
import re
import cbor2
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
from pydantic import BaseModel, Field, model_validator
from rembus.settings import broker_dir, rembus_dir
from rembus.protocol import tag2df, df2tag, timestamp, PubSubMsg

logger = logging.getLogger(__name__)


typemap = {
    "BLOB": pl.Binary,
    "TEXT": pl.Utf8,  # String
    "UTINYINT": pl.UInt8,
    "SMALLINT": pl.Int16,
    "INTEGER": pl.Int32,
    "HUGEINT": pl.Int128
    if hasattr(pl, "Int128")
    else pl.Int64,  # Int128 fallback
    "USMALLINT": pl.UInt16,
    "UINTEGER": pl.UInt32,
    "UBIGINT": pl.UInt64,
    "UHUGEINT": pl.UInt128
    if hasattr(pl, "UInt128")
    else pl.UInt64,  # UInt128 fallback
    "FLOAT": pl.Float32,
    "DOUBLE": pl.Float64,
    "TIMESTAMP": pl.Datetime,
}


class Column(BaseModel):
    col: str
    type: str
    nullable: bool = True
    default: Optional[Any] = None


class Table(BaseModel):
    table: str
    columns: List[Column] = Field(default_factory=list)
    keys: List[str] = Field(default_factory=list)
    extras: Dict[str, Any] = Field(default_factory=dict)
    topic: Optional[str] = None
    delete_topic: Optional[str] = None

    @model_validator(mode="after")
    def set_default_topic(self):
        if self.topic is None:
            object.__setattr__(self, "topic", self.table)
        return self


class Schema(BaseModel):
    tables: List[Table]


def column_to_sql(c: Column) -> str:
    """Convert a Column object to its SQL representation."""
    default_part = ""
    if c.default is not None:
        if isinstance(c.default, str):
            default_part = f" DEFAULT '{c.default}'"
        else:
            default_part = f" DEFAULT {c.default}"

    null_part = "" if c.nullable else " NOT NULL"

    return f"{c.col} {c.type}{null_part}{default_part}"


def create_table_sql(t: Table) -> str:
    """Generate the SQL statement to create a table."""
    fields = [column_to_sql(c) for c in t.columns]

    if "recv_ts" in t.extras:
        cn = t.extras["recv_ts"]
        fields.append(f"{cn} UBIGINT NOT NULL")
    if "slot" in t.extras:
        cn = t.extras["slot"]
        fields.append(f"{cn} UINTEGER")

    cols_sql = ",".join(fields)
    sql = f"CREATE TABLE IF NOT EXISTS {t.table} ({cols_sql});"
    return sql


def parse_dburl():
    raw = os.environ["DUCKLAKE_URL"]
    # remove prefix "postgres:"
    _, _, url = raw.split(":", 2)
    o = urlparse(url)
    return [o.username, o.password, o.hostname, o.path.lstrip("/")]


def reset_db(broker_name):
    dl_url = os.environ.get("DUCKLAKE_URL")

    if dl_url:
        _, _, _, db = parse_dburl()
        if dl_url.startswith("ducklake:postgres"):
            subprocess.run(["dropdb", db, "--if-exists"], check=False)
            subprocess.run(["createdb", db], check=True)

        elif dl_url.startswith("ducklake:sqlite"):
            logger.debug("removing db %s", db)
            Path(db).unlink(missing_ok=True)
    else:
        broker_ducklake = Path(rembus_dir()) / f"{broker_name}.ducklake"
        broker_ducklake.unlink(True)

    broker_folder = Path(broker_dir(broker_name)) / "main"

    if broker_folder.exists() and broker_folder.is_dir():
        shutil.rmtree(broker_folder)


def init_db(router, schema):
    """Initialize the database for a given router."""
    db = duckdb.connect()
    db.sql("INSTALL ducklake")
    db.sql(router.config.db_attach)
    db.sql("USE rl")

    tables = [
        """
        CREATE TABLE IF NOT EXISTS message (
            name TEXT NOT NULL,
            recv UBIGINT,
            slot UINTEGER,
            qos UTINYINT,
            uid UBIGINT,
            topic TEXT NOT NULL,
            data BLOB
        )""",
        """
        CREATE TABLE IF NOT EXISTS exposer (
            name TEXT NOT NULL,
            twin TEXT NOT NULL,
            topic TEXT NOT NULL,
        )""",
        """
        CREATE TABLE IF NOT EXISTS subscriber (
            name TEXT NOT NULL,
            twin TEXT NOT NULL,
            topic TEXT NOT NULL,
            msg_from DOUBLE
        )""",
        """
        CREATE TABLE IF NOT EXISTS mark (
            name TEXT NOT NULL,
            twin TEXT NOT NULL,
            mark UBIGINT,
        )""",
        """
        CREATE TABLE IF NOT EXISTS admin (
            name TEXT NOT NULL,
            twin TEXT
        )""",
        """
        CREATE TABLE IF NOT EXISTS topic_auth (
            name TEXT NOT NULL,
            twin TEXT NOT NULL,
            topic TEXT NOT NULL
        )""",
        """
        CREATE TABLE IF NOT EXISTS tenant (
            name TEXT NOT NULL,
            twin TEXT NOT NULL,
            secret TEXT NOT NULL
        )""",
        """
        CREATE TABLE IF NOT EXISTS wait_ack2 (
            name TEXT NOT NULL,
            twin TEXT NOT NULL,
            ts UBIGINT,
            id UBIGINT
        )
        """,
    ]

    for table in tables:
        db.sql(table)

    if schema:
        with open(
            schema,
            "r",
            encoding="utf-8",
        ) as f:
            data = json.load(f)

        sch = Schema(**data)
        router.tables = {tbl.table: tbl for tbl in sch.tables}
        for table in sch.tables:
            tname = table.table
            sql = create_table_sql(table)
            logger.debug("creating table %s: %s", tname, sql)
            db.execute(sql)
            # Create the query and delete rpc topics.
            router.handler[f"query_{tname}"] = partial(query, router, tname)
            router.handler[f"delete_{tname}"] = partial(delete, router, tname)

    return db


def delete(router, table, obj=None, ctx=None, node=None):
    """Delete rows from `table` matching conditions in `obj["where"]`."""
    cond_str = None
    if obj is not None:
        allowed = ("where", "when")
        bad = [k for k in obj.keys() if k not in allowed]

        if bad:
            raise ValueError(f"invalid keys: {', '.join(bad)}")

        cond_str = obj.get("where", None)

    if cond_str:
        sql = f"DELETE FROM {table} WHERE {cond_str}"
    else:
        sql = f"DELETE FROM {table}"

    logger.debug("db delete: %s", sql)
    router.db.execute(sql)


def query(router, table, obj=None, ctx=None, node=None):
    """Select rows from `table` matching conditions in `obj`."""
    if obj is None:
        sql = f"SELECT * FROM {table}"
    else:
        allowed = ("where", "when")
        bad = [k for k in obj.keys() if k not in allowed]
        if bad:
            raise ValueError(f"invalid keys: {', '.join(bad)}")
        where_cond = ""
        if "where" in obj:
            where_cond = " WHERE " + obj["where"]
        at = ""
        if "when" in obj:
            if isinstance(obj["when"], (int, float)):
                dt = datetime.fromtimestamp(obj["when"])
                ts = dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                ts = obj["when"]
            at = f" AT (TIMESTAMP => CAST('{ts}' AS TIMESTAMP))"
        sql = f"SELECT * FROM {table} {at} {where_cond}"

    logger.debug("db query: %s", sql)
    return router.db.execute(sql).pl()


def get_format(msg):
    """Return the format of the message data."""
    data = msg.data

    fmt = "sequence"

    if len(data) == 1:
        obj = data[0]
        if isinstance(obj, dict):
            return "key_value"
        elif isinstance(obj, cbor2.CBORTag):
            return "dataframe"

    # a hierarchy topic requires a dictionary or a dataframe as payload
    if msg.regex is not None and fmt == "sequence":
        return "invalid_format" if data else "key_value"

    return fmt


def getobj(topic, values):
    if not values:
        return dict()

    v = values[0]
    return dict(v)


def set_default(msg: PubSubMsg, tabledef: Table, d: dict, add_nullable=True):
    """
    Update dictionary `d` with defaults values.
    """
    for col in tabledef.columns:
        name = col.col

        # already present
        if name in d:
            continue

        # extract pieces from topic if applicable (msg.regex is set)
        topic_map = expand(msg)

        if name in topic_map:
            d[name] = topic_map[name]
            continue

        # column has an explicit default
        if col.default is not None:
            d[name] = col.default
            continue

        # nullable field (and not a primary key)
        if add_nullable and col.nullable and name not in tabledef.keys:
            d[name] = None
            continue


def df_extras(tabledef, df, msg):
    """Add extra columns (recvts, slot) to the end of df."""
    exprs = []
    extras_cols = []

    if "recv_ts" in tabledef.extras:
        cname = tabledef.extras["recv_ts"]
        exprs.append(pl.lit(msg.recvts).cast(pl.UInt64).alias(cname))
        extras_cols.append(cname)

    if "slot" in tabledef.extras:
        cname = tabledef.extras["slot"]
        exprs.append(pl.lit(msg.slot).cast(pl.UInt32).alias(cname))
        extras_cols.append(cname)

    # If no extras, return early
    if not exprs:
        return df

    # Add all extra columns in a single pass
    df = df.with_columns(exprs)

    # Reorder so new columns appear at the end
    current = [c for c in df.columns if c not in extras_cols]
    df = df.select(current + extras_cols)

    return df


def extras(tabledef, msg):
    vals = []
    if "recv_ts" in tabledef.extras:
        vals.append(msg.recvts)

    if "slot" in tabledef.extras:
        vals.append(msg.slot)

    return vals


def columns(table):
    return [t.col for t in table.columns]


def append(con: duckdb.DuckDBPyConnection, tabledef, msgs):
    # logger.debug("[%s] appending:\n%s", tabledef.table, df)
    topic = tabledef.table
    tblfields = columns(tabledef)
    logger.debug("[%s] appending columns: %s", topic, tblfields)
    all_rows = []  # will become a DataFrame batch

    for msg in msgs:
        fmt = get_format(msg)
        values = msg.data
        # key_value
        if fmt == "key_value":
            obj = getobj(topic, values)
            set_default(msg, tabledef, obj, add_nullable=True)
            # Check required fields
            if not all(k in obj for k in tblfields):
                logger.warning(
                    "[%s] unsaved %s with missed fields", topic, obj)
                continue
            fields = [obj[f] for f in tblfields]
        # dataframe
        elif fmt == "dataframe":
            df = tag2df(values[0])
            if list(df.columns) != tblfields:
                logger.warning(
                    "[%s] unsaved df with mismatched fields [%s]",
                    topic,
                    tblfields,
                )
                continue
            df = df_extras(tabledef, df, msg)
            con.register("df_view", df)
            con.execute(f"INSERT INTO {topic} SELECT * FROM df_view")
            con.unregister("df_view")
            continue
        # default format
        else:
            if len(values) != len(tblfields):
                logger.warning(
                    "[%s] unsaved %s with mismatched fields", topic, values
                )
                continue
            fields = values
        # Append extras
        extra_vals = extras(tabledef, msg)
        all_rows.append(fields + extra_vals)

    if not all_rows:
        return

    logger.debug("[%s] appending %d rows", topic, len(all_rows))
    # Build final DataFrame batch
    col_names = tblfields + list(tabledef.extras.values())
    batch_df = pd.DataFrame(all_rows, columns=col_names)
    # logger.debug("[%s] appending df:\n%s", topic, batch_df)
    con.register("batch_view", batch_df)
    con.execute(f"INSERT INTO {topic} SELECT * FROM batch_view")
    con.unregister("batch_view")


def schema_to_polars(tabledef: Table):
    """Convert table schema to Polars schema dictionary."""
    schema = {}
    for col in tabledef.columns:
        col_name = col.col
        col_type = typemap.get(col.type.upper(), pl.Utf8)  # default to Utf8
        schema[col_name] = col_type

    # Add extras
    if "recv_ts" in tabledef.extras:
        extra_col = tabledef.extras["recv_ts"]
        if extra_col not in schema:
            schema[extra_col] = typemap["UBIGINT"]

    if "slot" in tabledef.extras:
        extra_col = tabledef.extras["slot"]
        if extra_col not in schema:
            schema[extra_col] = typemap["UINTEGER"]

    return schema


def add_extras(table, msg, obj):
    if "recv_ts" in table.extras:
        obj[table.extras["recv_ts"]] = msg.recvts
    if "slot" in table.extras:
        obj[table.extras["slot"]] = msg.slot


def handle_key_value(msg, table, col_names, records, tname):
    obj = getobj(tname, msg.data)
    set_default(msg, table, obj, add_nullable=True)
    add_extras(table, msg, obj)

    if all(k in obj for k in col_names):
        records.append(obj)
    else:
        logger.warning(
            "[%s] unsaved %s missing required fields %s", tname, obj, col_names
        )


def handle_dataframe(msg, table, col_names, dataframes, tname):
    df = tag2df(msg.data[0])
    df = df_extras(table, df, msg)

    actual_cols = set(df.columns)
    expected_cols = set(col_names)

    missing = expected_cols - actual_cols
    if missing:
        logger.warning(
            "[%s] missing columns %s (expected %s, got %s)",
            tname,
            missing,
            col_names,
            df.columns,
        )
        return

    # Reorder columns to the expected order
    df = df.select(col_names)
    dataframes.append(df)


def handle_default(msg, table, col_names, records, tname):
    vals = msg.data
    if len(vals) != len(table.columns):
        logger.warning("[%s] unsaved %s with mismatched fields", tname, vals)
        return

    extra_vals = extras(table, msg)
    records.append(vals + extra_vals)


def execute_upsert(con, table, col_names, indexes, records, dataframes):
    tname = table.table

    tdf = pl.DataFrame(records, schema=schema_to_polars(table), orient="row")
    tdf = pl.concat([tdf, *dataframes], how="vertical")

    if tdf.is_empty():
        return

    if indexes:
        tdf = tdf.sort(indexes).group_by(indexes).agg([pl.all().last()])

    con.register("df_view", tdf)

    cond_str = " AND ".join(f"df_view.{k} = {tname}.{k}" for k in indexes)
    col_list = ", ".join(col_names)
    val_list = ", ".join(f"df_view.{c}" for c in col_names)

    update_cols = [c for c in col_names if c not in indexes]
    update_list = ", ".join(f"{c} = df_view.{c}" for c in update_cols)

    sql = f"""
        MERGE INTO {tname}
        USING df_view
        ON {cond_str}
        WHEN MATCHED THEN UPDATE SET {update_list}
        WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})
    """

    con.execute(sql)
    con.unregister("df_view")


def upsert(con: duckdb.DuckDBPyConnection, table, messages):
    """Insert/update a record in a table with keywords."""
    tname = table.table
    col_names = columns(table) + list(table.extras.values())
    indexes = list(table.keys)

    records = []
    dataframes = []

    for msg in messages:
        fmt = get_format(msg)
        #        try:
        if fmt == "key_value":
            handle_key_value(msg, table, col_names, records, tname)
        elif fmt == "dataframe":
            handle_dataframe(msg, table, col_names, dataframes, tname)
        else:
            handle_default(msg, table, col_names, records, tname)

    #        except Exception as e:  # pylint: disable=broad-except
    #            logger.error("[upsert] %s: %s", tname, e)

    execute_upsert(con, table, col_names, indexes, records, dataframes)


def expand(msg: PubSubMsg):
    """
    Given a message `msg` containing a topic string
    (e.g. `veneto/agordo/temperature`) and a regex value
    (e.g. `:regione/:loc/temperature`) extract the named parts from
    `msg.topic` according to `msg.regex` and add them as new attributes
    to `msg`.
    """
    d = {}
    if msg.regex is not None:
        names = extract_names(msg.regex)
        regex = make_regex(msg.regex)

        match = regex.match(msg.topic)
        if match:
            for name in names:
                d[name] = match.group(name)
    return d


def extract_names(pattern: str):
    """
    Extract placeholder names from a pattern like ':regione/:loc/temp'
    → ['regione', 'loc']
    """
    return [token[1:] for token in pattern.split("/") if token.startswith(":")]


def make_regex(pattern: str):
    """
    Convert pattern into a Python regular expression.

    Example:
        ':regione/:loc/temperature'
    becomes:
        '^(?P<regione>[^/]+)/(?P<loc>[^/]+)/temperature$'
    """
    parts = []
    for token in pattern.split("/"):
        if token.startswith(":"):  # placeholder
            name = token[1:]
            parts.append(f"(?P<{name}>[^/]+)")
        else:
            parts.append(re.escape(token))
    return re.compile("^" + "/".join(parts) + "$")


def msg_table(router, msg: PubSubMsg):
    """
    Given a router with defined tables and a pubsub message `msg`, this
    function matches the topic against the router's table
    patterns and assigns the corresponding table name and pattern.
    If no pattern matches, the topic name itself is used as the table name and
    `nothing` for the pattern.
    """
    schema_tables = router.tables.values()

    topic = msg.topic
    topic_tokens = topic.split("/")
    msg.table = topic
    for schema_table in schema_tables:
        schema_topic = schema_table.topic
        schema_tokens = schema_topic.split("/")
        # Must have same number of segments
        if len(topic_tokens) != len(schema_tokens):
            continue
        # token-by-token comparison
        ok = True
        is_param = False
        for idx, schema_token in enumerate(schema_tokens):
            # Literal match required when schema token does NOT start with ':'
            if schema_token.startswith(":"):
                is_param = True
            else:
                if schema_token != topic_tokens[idx]:
                    ok = False
                    break
        if ok:
            msg.table = schema_table.table
            if is_param:
                msg.regex = schema_topic
            break


def save_data_at_rest(router):
    """Save cached messages to the database."""
    # logger.debug("[%s] saving %d messages", router, len(router.msg_cache))
    msgs = router.msg_cache
    if not msgs:
        return

    batch = build_message_batch(router.id, msgs)
    (router.db.from_arrow(batch).insert_into("message"))

    for topic, msgs in router.msg_topic_cache.items():
        table = router.tables[topic]
        if table.keys:
            upsert(router.db, table, msgs)
        else:
            append(router.db, table, msgs)

    router.msg_cache.clear()
    router.msg_topic_cache.clear()


async def send_messages(twin, df, ts):
    r = twin.router
    for name, recv, slot, qos, uid, topic, data in df.iter_rows():
        if (
            recv > twin.mark
            and recv > ts - twin.msg_from.get(topic, 0)
            and topic in r.subscribers
            and twin in r.subscribers[topic]
        ):
            payload = cbor2.loads(data)
            if payload:
                await twin.publish(topic, *payload, slot=slot, qos=qos)
            else:
                await twin.publish(topic, slot=slot, qos=qos)
            twin.mark = recv


async def send_data_at_rest(msg, max_period=3600_000_000_000):
    twin = msg.twin
    r = twin.router
    db = twin.db

    ts = timestamp()
    if twin.uid.hasname:
        logger.debug("[%s] sending data at rest", twin)
        df = db.execute(
            f"SELECT * FROM message WHERE name='{r.id}' AND recv>={ts - max_period}"
        ).pl()
        await send_messages(twin, df, ts)


def build_message_batch(broker_id: str, msgs: list):
    """Build a PyArrow Table from a list of message tuples."""

    return pa.table(
        {
            "name": [broker_id] * len(msgs),
            "recv": [m.recvts for m in msgs],
            "slot": [m.slot for m in msgs],
            "qos": [m.flags for m in msgs],
            "uid": [m.id for m in msgs],
            "topic": [m.topic for m in msgs],
            "data": [cbor2.dumps(df2tag(m.data)) for m in msgs],
        },
        schema=pa.schema(
            {
                "name": pa.string(),
                "recv": pa.int64(),
                "slot": pa.int64(),
                "qos": pa.int64(),
                "uid": pa.uint64(),
                "topic": pa.string(),
                "data": pa.binary(),
            }
        ),
    )
