#!/usr/bin/env python3

import argparse
import rembus as rb


def add_tenant(tenant: str, secret: str, broker_name: str = "broker"):
    print(f"Adding tenant [{tenant}]")
    db = rb.connect_db(broker_name)

    try:
        db.execute("""
        CREATE TABLE IF NOT EXISTS tenant (
            name TEXT NOT NULL,
            twin TEXT NOT NULL,
            secret TEXT NOT NULL)
        """)

        db.execute("""
            DELETE FROM tenant
            WHERE name = ? AND twin = ?
        """, (broker_name, tenant))

        db.execute("""
        INSERT INTO tenant (name, twin, secret)
        VALUES (?, ?, ?)
        """, (broker_name, tenant, secret))
    except Exception as e:
        print(f"Add tenant [{tenant}] failed: {e}")
    finally:
        db.close()


def remove_tenant(tenant: str, broker_name: str = "broker"):
    print(f"Removing tenant [{tenant}]")
    db = rb.connect_db(broker_name)

    try:
        db.execute("""
        DELETE FROM tenant WHERE name=? and twin=?)
        """, (broker_name, tenant))
    except Exception as e:
        print(f"Remove tenant [{tenant}] failed: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage Rembus tenants")

    parser.add_argument(
        "tenant", help="tenant name"
    )
    parser.add_argument(
        "-d", "--delete", action="store_true", help="remove tenant"
    )
    parser.add_argument(
        "-s", "--secret", help="tenant secret"
    )
    parser.add_argument(
        "--broker", default="broker", help="broker name (default: broker)"
    )

    args = parser.parse_args()

    if args.delete:
        remove_tenant(args.tenant, args.broker)
    else:
        add_tenant(args.tenant, args.secret, args.broker)
