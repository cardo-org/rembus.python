"""
Server that exposes an "export_zip" RPC topic.

Given a directory path, it zips the directory's content in memory and
returns the zip archive as raw bytes. cbor2 (used by rembus for the
wire encoding) natively supports Python `bytes`, so no extra encoding
step is needed: the caller receives the archive as a `bytes` object.
"""
import asyncio
import io
import os
import zipfile

import rembus


async def export_zip(dirpath: str) -> bytes:
    """Zip the content of dirpath and return the archive bytes."""
    if not os.path.isdir(dirpath):
        raise ValueError(f"not a directory: {dirpath}")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(dirpath):
            for fname in files:
                fullpath = os.path.join(root, fname)
                # store paths relative to dirpath inside the archive
                arcname = os.path.relpath(fullpath, start=dirpath)
                zf.write(fullpath, arcname)

    return buffer.getvalue()


async def main():
    rb = await rembus.component(port=8338)
    await rb.expose(export_zip, topic="export_zip")
    await rb.wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nbye")
