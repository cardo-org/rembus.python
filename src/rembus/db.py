import os
import logging
from typing import Any, Dict, List, Optional
import json
import re
import cbor2
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
from pydantic import BaseModel, Field, model_validator
from rembus.settings import rembus_dir
from rembus.protocol import tag2df, df2tag

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
    format: str = "sequence"
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


def init_db(router, schema):
    """Initialize the database for a given router."""
    data_dir = os.path.join(rembus_dir(), router.id)
    if "DUCKLAKE_URL" in os.environ:
        db_name = os.environ["DUCKLAKE_URL"]
    else:
        db_name = f"{data_dir}.ducklake"
    db = duckdb.connect()
    db.sql("INSTALL ducklake")
    logger.debug(
        "ATTACH 'ducklake:%s' AS rl (DATA_PATH '%s')", db_name, data_dir
    )
    db.sql(f"ATTACH 'ducklake:{db_name}' AS rl (DATA_PATH '{data_dir}')")
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
            data TEXT
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
            sql = create_table_sql(table)
            logger.debug("creating table %s: %s", table.table, sql)
            db.execute(sql)

    return db


def getobj(topic, values):
    v = values[0]
    if not isinstance(v, dict):
        raise ValueError("[format is key_value: data must be a dictionary")
    return dict(v)


def set_default(row, tabledef, d, add_nullable=True):
    """
    row: Pandas Series
    tabledef: Table (Pydantic model)
    d: dict that will receive defaults
    """
    for col in tabledef.columns:
        name = col.col

        # already present
        if name in d:
            continue

        # present in a row column
        col_key = name
        if col_key in row and pd.notna(row[col_key]):
            d[name] = row[col_key]
            continue

        # column has an explicit default
        if col.default is not None:
            d[name] = col.default
            continue

        # nullable field (and not a primary key)
        if add_nullable and col.nullable and name not in tabledef.keys:
            d[name] = None
            continue


def df_extras(tabledef, df, row):
    """Add extra columns (recv_ts, slot) to the end of df."""
    exprs = []
    extras_cols = []

    if "recv_ts" in tabledef.extras:
        cname = tabledef.extras["recv_ts"]
        exprs.append(pl.lit(row["recv"]).cast(pl.UInt64).alias(cname))
        extras_cols.append(cname)

    if "slot" in tabledef.extras:
        cname = tabledef.extras["slot"]
        exprs.append(pl.lit(row["slot"]).cast(pl.UInt32).alias(cname))
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


# TODO: a more efficient version that avoids creating a new Series per column
# if df is large — using pl.lit() in expressions instead
def df_extras_old(tabledef, df, row):
    new_cols = []

    if "recv_ts" in tabledef.extras:
        col = tabledef.extras["recv_ts"]
        new_cols.append(
            pl.Series(name=col, values=[row["recv"]] * len(df), dtype=pl.UInt64)
        )
    if "slot" in tabledef.extras:
        col = tabledef.extras["slot"]
        new_cols.append(
            pl.Series(name=col, values=[row["slot"]] * len(df), dtype=pl.UInt32)
        )

    if new_cols:
        df = df.with_columns(new_cols)

    return df


def extras(tabledef, fields, row):
    vals = []

    # If table has extra "recv_ts", append row.recv
    if "recv_ts" in tabledef.extras:
        vals.append(row["recv"])

    # If table has extra "slot", append row.slot
    if "slot" in tabledef.extras:
        vals.append(row["slot"])

    return vals


def columns(table):
    return [t.col for t in table.columns]


def append(con: duckdb.DuckDBPyConnection, tabledef, df: pd.DataFrame):
    # logger.debug("[%s] appending:\n%s", tabledef.table, df)
    topic = tabledef.table
    fmt = tabledef.format
    tblfields = columns(tabledef)
    logger.debug("columns: %s", tblfields)
    all_rows = []  # will become a DataFrame batch

    for _, row in df.iterrows():
        try:
            values = row["data"]
            # key_value format
            if fmt == "key_value":
                obj = getobj(topic, values)
                set_default(row, tabledef, obj, add_nullable=True)

                # Check required fields
                if not all(k in obj for k in tblfields):
                    logger.warning(
                        "[%s] unsaved %s with missed fields", topic, obj
                    )
                    continue

                fields = [obj[f] for f in tblfields]

            # dataframe format
            elif fmt == "dataframe":
                df2 = tag2df(values[0])
                if list(df2.columns) != tblfields:
                    logger.warning(
                        "[%s] unsaved df with mismatched fields %s",
                        topic,
                        tblfields,
                    )
                    continue

                df2 = df_extras(tabledef, df2, row)

                con.register("df_view", df2)
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
            extra_vals = extras(tabledef, fields, row)

            all_rows.append(fields + extra_vals)
        except Exception as e:
            logger.error("[append] %s: %s", topic, e)

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


def upsert(con: duckdb.DuckDBPyConnection, tabledef, df: pd.DataFrame):
    tname = tabledef.table
    fmt = tabledef.format
    col_names = columns(tabledef) + list(tabledef.extras.values())
    indexes = list(tabledef.keys)
    tdf = pl.DataFrame([], schema=schema_to_polars(tabledef))

    # Build final DataFrame (tdf)
    for _, row in df.iterrows():
        try:
            values = row["data"]

            # key_value
            if fmt == "key_value":
                obj = getobj(tname, values)
                set_default(row, tabledef, obj, add_nullable=True)

                if "recv_ts" in tabledef.extras:
                    obj[tabledef.extras["recv_ts"]] = row.recv
                if "slot" in tabledef.extras:
                    obj[tabledef.extras["slot"]] = row.slot

                # ensure all fields exist
                if all(k in obj for k in col_names):
                    row_df = pl.DataFrame(
                        obj, schema=schema_to_polars(tabledef)
                    )[col_names]  # reorder columns

                    tdf = pl.concat([tdf, row_df], how="vertical")
                else:
                    logger.warning(
                        "[%s] unsaved %s missing required fields %s",
                        tname,
                        obj,
                        col_names,
                    )
                    continue

            # dataframe
            elif fmt == "dataframe":
                df2 = tag2df(values[0])
                df2 = df_extras(tabledef, df2, row)
                if list(df2.columns) == col_names:
                    tdf = pl.concat([tdf, df2], how="vertical")
                else:
                    logger.warning(
                        "[%s] unsaved df2 (mismatched fields)", tname
                    )
                    continue

            # default format
            else:
                vals = values
                if len(vals) == len(tabledef.columns):
                    extra_vals = extras(tabledef, vals, row)
                    all_vals = vals + extra_vals

                    row_df = pl.DataFrame(
                        [dict(zip(col_names, all_vals))],
                        schema=schema_to_polars(tabledef),
                    )
                    tdf = pl.concat([tdf, row_df], how="vertical")
                else:
                    logger.warning(
                        "[%s] unsaved %s with mismatched fields", tname, vals
                    )
                    continue
        except Exception as e:
            logger.error("[upsert] %s: %s", tname, e)

    if tdf.is_empty():
        return

    if indexes:
        tdf = (
            tdf.sort(indexes)  # sort so last row is last
            .group_by(indexes)
            .agg([pl.all().last()])  # take last row of each group
        )

    logger.debug("[%s] upserting dataframe:\n%s", tname, tdf)

    con.register("df_view", tdf)

    conds = [f"df_view.{k} = {tname}.{k}" for k in indexes]
    cond_str = " AND ".join(conds)

    col_list = ", ".join(col_names)
    val_list = ", ".join(f"df_view.{c}" for c in col_names)

    update_columns = [c for c in col_names if c not in indexes]
    update_list = ", ".join(f"{c} = df_view.{c}" for c in update_columns)

    sql = f"""
        MERGE INTO {tname}
        USING df_view
        ON {cond_str}
        WHEN MATCHED THEN UPDATE SET {update_list}
        WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list})
    """

    con.execute(sql)
    con.unregister("df_view")


def expand(df: pd.DataFrame):
    """
    Given a DataFrame `df` containing a topic string column (e.g. `:topic`)
    and a pattern column (e.g. `:regexp`) with placeholders like
    `:regione/:loc/temperature`, this function extracts the named parts from
    `topic` according to the `regexp`and adds them as new columns to `df`.

    Example:
        df = DataFrame(
            topic=["veneto/agordo/temperature", "veneto/feltre/temperature"],
            table=["temperature", "temperature"],
            regexp=[":regione/:loc/temperature", ":regione/:loc/temperature"]
        )

        expand!(df, :topic, :regexp)
    """
    pattern = df.iloc[0]["regexp"]
    if pattern is None:
        return df

    names = extract_names(pattern)
    regex = make_regex(pattern)

    extracted_columns = {name: [] for name in names}

    # For each row, extract variables
    for _, row in df.iterrows():
        topic = row["topic"]
        match = regex.match(topic)
        if match:
            for name in names:
                extracted_columns[name].append(match.group(name))

    # Add new columns to df
    for name in names:
        df[name] = extracted_columns[name]

    return df


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


def settable(router, df):
    """
    Given a router with defined tables and a DataFrame `df` containing a
    `:topic` column, this function matches each topic against the router's table
    patterns and assigns the corresponding table name and pattern to new
    `:table` and `:regexp` columns in `df`.
    If no pattern matches, the topic name itself is used as the table name and
    `nothing` for the pattern.
    """
    schema_tables = router.tables.values()

    tables_out = []
    regexps_out = []

    for _, msg in df.iterrows():
        topic = msg["topic"]
        topic_tokens = topic.split("/")

        matched = False

        for schema_table in schema_tables:
            schema_topic = schema_table.topic
            schema_tokens = schema_topic.split("/")

            # must have same number of segments
            if len(topic_tokens) != len(schema_tokens):
                continue

            # token-by-token comparison
            ok = True
            is_param = False
            for idx, schema_token in enumerate(schema_tokens):
                # literal match required when schema token does NOT start with ':'
                if schema_token.startswith(":"):
                    is_param = True
                else:
                    if schema_token != topic_tokens[idx]:
                        ok = False
                        break

            if ok:
                matched = True
                tables_out.append(schema_table.table)  # table name
                if is_param:
                    regexps_out.append(schema_topic)  # regexp pattern topic
                else:
                    regexps_out.append(None)
                break

        if not matched:
            # fallback: table equals the raw message topic
            tables_out.append(topic)
            regexps_out.append(None)

    # Add columns to dataframe
    df["table"] = tables_out
    df["regexp"] = regexps_out

    return df  # optional, since df modified in-place


def save_data_at_rest(router):
    """Save cached messages to the database."""
    logger.debug(
        "[save_data_at_rest] saving %d messages", len(router.msg_cache)
    )
    msgs = router.msg_cache
    if not msgs:
        return

    batch = build_message_batch(router.id, msgs)

    (router.db.from_arrow(batch).insert_into("message"))

    # create per-table batches
    records = [
        {
            "recv": m.recvts,
            "slot": m.slot,
            "qos": m.flags,
            "uid": m.id,
            "topic": m.topic,
            "data": m.data,
        }
        for m in msgs
    ]
    df = pd.DataFrame(records)
    settable(router, df)
    logger.debug("[save_data_at_rest] initial df:\n%s", df)

    for tbl in df["table"].unique():
        if tbl in router.tables:
            logger.debug("[save_data_at_rest][%s] processing df\n%s", tbl, df)
            topicdf = df[df["table"] == tbl].copy()
            if not topicdf.empty:
                expand(topicdf)
                tabledef = router.tables[tbl]
                # if isempty(tabledef.keys)
                if not tabledef.keys:
                    append(router.db, tabledef, topicdf)
                else:
                    upsert(router.db, tabledef, topicdf)

    router.msg_cache.clear()


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
