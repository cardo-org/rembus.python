"""Operator-side provisioning of the registration secrets.

A registration secret (the pin of ``Register`` and ``Rekey``) is associated
with a tenant and, optionally, with a ``cid``. For a ``cid`` the broker uses
its own secret if it has one, otherwise the secret of its tenant. A tenant is
in one mode, ``pin-proof`` or ``opaque-only``, and every secret of the tenant
and of its ``cid`` s must be in that mode.
"""

from contextlib import contextmanager

from . import opaque
from . import settings as rs
from .core import tenant_of, valid_cid
from .db import dbconnect

MODE_PIN = "pin-proof"
MODE_OPAQUE = "opaque-only"
MODES = (MODE_PIN, MODE_OPAQUE)

_TABLES = (
    "CREATE TABLE IF NOT EXISTS tenant ("
    "name TEXT NOT NULL, tenant TEXT NOT NULL, secret TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS tenant_opaque ("
    "name TEXT NOT NULL, tenant TEXT NOT NULL, password_file BLOB NOT NULL)",
    "CREATE TABLE IF NOT EXISTS cid_secret ("
    "name TEXT NOT NULL, cid TEXT NOT NULL, secret TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS cid_opaque ("
    "name TEXT NOT NULL, cid TEXT NOT NULL, password_file BLOB NOT NULL)",
    "CREATE TABLE IF NOT EXISTS tenant_mode ("
    "name TEXT NOT NULL, tenant TEXT NOT NULL, mode TEXT NOT NULL)",
)


@contextmanager
def _transaction(broker_name: str):
    """Open the database and run the block as one atomic change."""
    db = dbconnect(broker_name)
    try:
        for ddl in _TABLES:
            db.execute(ddl)
        db.execute("BEGIN")
        try:
            yield db
        except BaseException:
            db.execute("ROLLBACK")
            raise
        db.execute("COMMIT")
    finally:
        db.close()


def _tenants_enabled(broker_name: str) -> bool:
    return bool(rs.Settings(broker_name).tenants_enabled)


def _check_cid(cid: str):
    if not valid_cid(cid):
        raise ValueError(f"invalid cid: {cid!r}")


def _held_modes(db, broker: str, tenant: str, enabled: bool) -> set[str]:
    """Modes of the secrets held for `tenant` and for its cids."""
    held = set()
    for table, col, mode in (
        ("tenant", "tenant", MODE_PIN),
        ("tenant_opaque", "tenant", MODE_OPAQUE),
    ):
        rows = db.execute(
            f"SELECT 1 FROM {table} WHERE name = ? AND {col} = ?",
            (broker, tenant),
        ).fetchall()
        if rows:
            held.add(mode)
    for table, mode in (
        ("cid_secret", MODE_PIN),
        ("cid_opaque", MODE_OPAQUE),
    ):
        for (cid,) in db.execute(
            f"SELECT cid FROM {table} WHERE name = ?", (broker,)
        ).fetchall():
            if tenant_of(cid, enabled) == tenant:
                held.add(mode)
    return held


def _check_mode(db, broker: str, tenant: str, enabled: bool, mode: str):
    """Refuse a secret that is not in the mode of its tenant."""
    row = db.execute(
        "SELECT mode FROM tenant_mode WHERE name = ? AND tenant = ?",
        (broker, tenant),
    ).fetchone()
    if row is not None:
        if row[0] != mode:
            raise ValueError(f"tenant [{tenant}] is {row[0]}, not {mode}")
        return
    other = {m for m in MODES if m != mode}
    if _held_modes(db, broker, tenant, enabled) & other:
        raise ValueError(
            f"tenant [{tenant}] holds secrets of the other mode: remove "
            "them before changing the mode"
        )


def set_tenant_mode(
    tenant: str, mode: str | None, broker_name: str = "broker"
):
    """Set the mode of `tenant`, or clear it (``None``) to derive it from
    the secrets held. A mode change is refused while secrets of the old mode
    exist for the tenant or any of its cids."""
    if mode is not None and mode not in MODES:
        raise ValueError(f"invalid mode: {mode!r}")
    enabled = _tenants_enabled(broker_name)
    with _transaction(broker_name) as db:
        if mode is not None:
            _check_mode(db, broker_name, tenant, enabled, mode)
        db.execute(
            "DELETE FROM tenant_mode WHERE name = ? AND tenant = ?",
            (broker_name, tenant),
        )
        if mode is not None:
            db.execute(
                "INSERT INTO tenant_mode (name, tenant, mode)"
                " VALUES (?, ?, ?)",
                (broker_name, tenant, mode),
            )


def add_tenant(tenant: str, secret: str, broker_name: str = "broker"):
    """Associate the pin `secret` (pin-proof mode) with `tenant`.

    Refused if the tenant is, or holds secrets of, the other mode.
    """
    enabled = _tenants_enabled(broker_name)
    with _transaction(broker_name) as db:
        _check_mode(db, broker_name, tenant, enabled, MODE_PIN)
        db.execute(
            "DELETE FROM tenant WHERE name = ? AND tenant = ?",
            (broker_name, tenant),
        )
        db.execute(
            "INSERT INTO tenant (name, tenant, secret) VALUES (?, ?, ?)",
            (broker_name, tenant, secret),
        )


def add_tenant_opaque(tenant: str, secret: str, broker_name: str = "broker"):
    """Associate an OPAQUE record of the pin `secret` with `tenant`.

    The pin is not stored. Refused if the tenant is, or holds secrets of,
    the other mode (``pin-proof``).
    """
    record = opaque.enroll(broker_name, tenant, secret)
    enabled = _tenants_enabled(broker_name)
    with _transaction(broker_name) as db:
        _check_mode(db, broker_name, tenant, enabled, MODE_OPAQUE)
        db.execute(
            "DELETE FROM tenant_opaque WHERE name = ? AND tenant = ?",
            (broker_name, tenant),
        )
        db.execute(
            "INSERT INTO tenant_opaque (name, tenant, password_file)"
            " VALUES (?, ?, ?)",
            (broker_name, tenant, record),
        )


def remove_tenant(tenant: str, broker_name: str = "broker"):
    """Remove the secret and the explicit mode of `tenant`.

    The secrets of the cids of the tenant are independent and are kept.
    """
    with _transaction(broker_name) as db:
        for table in ("tenant", "tenant_opaque", "tenant_mode"):
            db.execute(
                f"DELETE FROM {table} WHERE name = ? AND tenant = ?",
                (broker_name, tenant),
            )


def add_cid(cid: str, secret: str, broker_name: str = "broker"):
    """Associate the pin `secret` (pin-proof mode) with `cid`.

    The `cid` need not be registered. Refused if the tenant of `cid` is, or
    holds secrets of, the other mode.
    """
    _check_cid(cid)
    enabled = _tenants_enabled(broker_name)
    tenant = tenant_of(cid, enabled)
    with _transaction(broker_name) as db:
        _check_mode(db, broker_name, tenant, enabled, MODE_PIN)
        db.execute(
            "DELETE FROM cid_secret WHERE name = ? AND cid = ?",
            (broker_name, cid),
        )
        db.execute(
            "INSERT INTO cid_secret (name, cid, secret) VALUES (?, ?, ?)",
            (broker_name, cid, secret),
        )


def add_cid_opaque(cid: str, secret: str, broker_name: str = "broker"):
    """Associate an OPAQUE record of the pin `secret` with `cid`.

    The record is bound to ``cid:<cid>`` and the pin is not stored.
    """
    _check_cid(cid)
    enabled = _tenants_enabled(broker_name)
    tenant = tenant_of(cid, enabled)
    record = opaque.enroll(broker_name, opaque.cid_identifier(cid), secret)
    with _transaction(broker_name) as db:
        _check_mode(db, broker_name, tenant, enabled, MODE_OPAQUE)
        db.execute(
            "DELETE FROM cid_opaque WHERE name = ? AND cid = ?",
            (broker_name, cid),
        )
        db.execute(
            "INSERT INTO cid_opaque (name, cid, password_file)"
            " VALUES (?, ?, ?)",
            (broker_name, cid, record),
        )


def remove_cid(cid: str, broker_name: str = "broker"):
    """Remove the secret of `cid`: it does not unregister the cid, and its
    later Register and Rekey use the secret of its tenant."""
    with _transaction(broker_name) as db:
        for table in ("cid_secret", "cid_opaque"):
            db.execute(
                f"DELETE FROM {table} WHERE name = ? AND cid = ?",
                (broker_name, cid),
            )


def main(argv: list[str] | None = None):
    """Command line entry point (``rembus-secure``)."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="rembus-secure",
        description="Manage the registration secrets of tenants and cids",
    )
    parser.add_argument("name", help="tenant name, or cid with --cid")
    parser.add_argument(
        "-d", "--delete", action="store_true", help="remove the secret"
    )
    parser.add_argument("-s", "--secret", help="registration pin")
    parser.add_argument(
        "--cid",
        action="store_true",
        help="the name is a cid: associate the secret with the cid",
    )
    parser.add_argument(
        "--opaque",
        action="store_true",
        help="store an OPAQUE record instead of the plain secret",
    )
    parser.add_argument(
        "--mode",
        choices=MODES + ("derived",),
        help="set the mode of the tenant (derived: clear it)",
    )
    parser.add_argument(
        "--broker", default="broker", help="broker name (default: broker)"
    )

    args = parser.parse_args(argv)

    if args.mode:
        mode = None if args.mode == "derived" else args.mode
        set_tenant_mode(args.name, mode, args.broker)
    elif args.delete:
        (remove_cid if args.cid else remove_tenant)(args.name, args.broker)
    elif args.cid:
        (add_cid_opaque if args.opaque else add_cid)(
            args.name, args.secret, args.broker
        )
    elif args.opaque:
        add_tenant_opaque(args.name, args.secret, args.broker)
    else:
        add_tenant(args.name, args.secret, args.broker)


if __name__ == "__main__":
    main()
