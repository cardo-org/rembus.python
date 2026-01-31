
#!/usr/bin/env python3

import argparse
import rembus as rb

def add_admin(name:str, broker_name:str = "broker"):
    print(f"Adding [{name}] as admin")
    db = rb.connect_db(broker_name)

    try:
        db.execute("""
        CREATE TABLE IF NOT EXISTS admin (
            name TEXT NOT NULL,
            twin TEXT)
        """)
        
        db.execute("""
        DELETE FROM admin WHERE name=? and twin=?)
        """, (broker_name, name))

        db.execute("""
        INSERT INTO admin (name, twin)
        VALUES (?, ?)
        """, (broker_name, name))
    except Exception as e:
        print(f"Add admin [{name}] failed: {e}")
    finally:
        db.close()

def remove_admin(name:str, broker_name:str = "broker"):
    print(f"Removing [{name}] as admin")
    db = rb.connect_db(broker_name)

    try:
        db.execute("""
        DELETE FROM admin WHERE name=? and twin=?)
        """, (broker_name, name))
    except Exception as e:
        print(f"Remove admin [{name}] failed: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage Rembus admins")
    
    parser.add_argument(
        "admin_name", help="admin name"
    )
    parser.add_argument(
        "-d", "--delete", action="store_true", help="remove admin"
    )
    parser.add_argument(
        "--broker", default="broker", help="broker name (default: broker)"
    )

    args = parser.parse_args()

    if args.delete:
        remove_admin(args.admin_name, args.broker)
    else:
        add_admin(args.admin_name, args.broker)