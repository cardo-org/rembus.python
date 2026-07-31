"""
Client that calls the "export_zip" topic and saves the returned bytes
to a .zip file on disk.

Usage:
    python client.py <remote_dir> [output.zip]
"""
import sys

import rembus


def main():
    remote_dir = sys.argv[1] if len(sys.argv) > 1 else "mydir"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "export.zip"

    node = rembus.node("zip_client")
    data = node.rpc("export_zip", remote_dir)

    with open(out_path, "wb") as f:
        f.write(data)

    print(f"saved {len(data)} bytes to {out_path}")


if __name__ == "__main__":
    main()
