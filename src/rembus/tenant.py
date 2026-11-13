from . import opaque
from .db import dbconnect


def add_tenant(tenant: str, secret: str, broker_name: str = "broker"):
    """Register `tenant` with a plain shared `secret` (pin-proof mode).

    Any OPAQUE record of the tenant is removed: a tenant is in one mode only.
    """
    db = dbconnect(broker_name)
    try:
        db.execute("""
        CREATE TABLE IF NOT EXISTS tenant_opaque (
            name TEXT NOT NULL,
            tenant TEXT NOT NULL,
            password_file BLOB NOT NULL)
        """)
        db.execute(
            "DELETE FROM tenant_opaque WHERE name = ? AND tenant = ?",
            (broker_name, tenant),
        )
        db.execute("""
        CREATE TABLE IF NOT EXISTS tenant (
            name TEXT NOT NULL,
            tenant TEXT NOT NULL,
            secret TEXT NOT NULL)
        """)
        db.execute(
            "DELETE FROM tenant WHERE name = ? AND tenant = ?",
            (broker_name, tenant),
        )
        db.execute(
            "INSERT INTO tenant (name, tenant, secret) VALUES (?, ?, ?)",
            (broker_name, tenant, secret),
        )
    finally:
        db.close()


def add_tenant_opaque(tenant: str, secret: str, broker_name: str = "broker"):
    """Enable the tenant for OPAQUE registrations: the secret is not stored.

    The plain secret of the tenant, if any, is removed: a tenant is in one
    mode only (``opaque-only``).
    """
    record = opaque.enroll(broker_name, tenant, secret)
    db = dbconnect(broker_name)
    try:
        db.execute("""
        CREATE TABLE IF NOT EXISTS tenant (
            name TEXT NOT NULL,
            tenant TEXT NOT NULL,
            secret TEXT NOT NULL)
        """)
        db.execute(
            "DELETE FROM tenant WHERE name = ? AND tenant = ?",
            (broker_name, tenant),
        )
        db.execute("""
        CREATE TABLE IF NOT EXISTS tenant_opaque (
            name TEXT NOT NULL,
            tenant TEXT NOT NULL,
            password_file BLOB NOT NULL)
        """)
        db.execute(
            "DELETE FROM tenant_opaque WHERE name = ? AND tenant = ?",
            (broker_name, tenant),
        )
        db.execute(
            "INSERT INTO tenant_opaque (name, tenant, password_file)"
            " VALUES (?, ?, ?)",
            (broker_name, tenant, record),
        )
    finally:
        db.close()


def remove_tenant(tenant: str, broker_name: str = "broker"):
    """Remove `tenant` (plain and OPAQUE) from the broker."""
    db = dbconnect(broker_name)
    try:
        db.execute(
            "DELETE FROM tenant WHERE name=? AND tenant=?",
            (broker_name, tenant),
        )
        db.execute(
            "DELETE FROM tenant_opaque WHERE name=? AND tenant=?",
            (broker_name, tenant),
        )
    finally:
        db.close()


def main(argv: list[str] | None = None):
    """Command line entry point (``rembus-tenant``)."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="rembus-tenant", description="Manage Rembus tenants"
    )
    parser.add_argument("tenant", help="tenant name")
    parser.add_argument(
        "-d", "--delete", action="store_true", help="remove tenant"
    )
    parser.add_argument("-s", "--secret", help="tenant secret")
    parser.add_argument(
        "--opaque",
        action="store_true",
        help="store an OPAQUE record instead of the plain secret",
    )
    parser.add_argument(
        "--broker", default="broker", help="broker name (default: broker)"
    )

    args = parser.parse_args(argv)

    if args.delete:
        remove_tenant(args.tenant, args.broker)
    elif args.opaque:
        add_tenant_opaque(args.tenant, args.secret, args.broker)
    else:
        add_tenant(args.tenant, args.secret, args.broker)


if __name__ == "__main__":
    main()
