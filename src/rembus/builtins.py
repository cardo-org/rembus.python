"""
This module provides functions to manage distributed services and subscribers.
It allows to list, add, and remove callbacks.
The callbacks are stored as Python files in the .config/rembus/component/src
directory, and are dynamically loaded and exposed as services or subscribers.
"""

import logging
import importlib.util
import os
import sys
from pathlib import Path
import aiofiles
from rembus.settings import broker_dir

logger = logging.getLogger(__name__)


def callback_dir(router_id, cbtype):
    """Return the dir where are stored services implementations"""
    return Path(broker_dir(router_id)) / "src" / cbtype


async def eval_file(twin, cbtype, name, path):
    """
    Dynamically load a Python module from a file path and expose its service
    function.

    Args:
        twin: The twin object to expose the service to
        name: The service name
        path: Path to the Python file containing the service
    """
    # Create a module spec from the file
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {path}")

    # Create a new module
    mod = importlib.util.module_from_spec(spec)

    # Add to sys.modules so it can be imported by other code if needed
    sys.modules[name] = mod

    # Execute the module
    spec.loader.exec_module(mod)

    # Get the service function
    # (assuming it's named as module name)
    if hasattr(mod, name):
        service_fn = getattr(mod, name)
    else:
        # Get the first callable that's not a built-in
        service_fn = None
        for attr_name in dir(mod):
            if not attr_name.startswith("_"):
                attr = getattr(mod, attr_name)
                if callable(attr):
                    service_fn = attr
                    break
        if service_fn is None:
            # remove invalid file
            path.unlink(missing_ok=True)
            raise ValueError(f"no callback found in {path}")

    if cbtype == "services":
        await twin.expose(service_fn, name)
    else:
        await twin.subscribe(service_fn, name)

    return service_fn


def local_twin(router):
    """Get the repl twin or the solo twin if it is a connector."""
    if "repl" in router.id_twin:
        return router.id_twin["repl"]

    return router.id_twin[next(iter(router.id_twin))]


async def list_callback(router, cbtype, getbody=False):
    """
    Get the list of distributed services.

    Args:
        router: The router object
        getbody: Return the service code
    """
    # Get the broker directory and create the services directory
    dir_path = callback_dir(router.id, cbtype)

    if not os.path.exists(dir_path):
        return []

    files = []
    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        if os.path.isfile(file_path):
            if getbody:
                async with aiofiles.open(file_path, "r") as f:
                    body = await f.read()
                files.append({"name": filename, "body": body})
            else:
                files.append({"name": filename})

    return files


async def add_callback(router, cbtype, cfg):
    """
    Save service/subscriber code to a file and dynamically load it.

    Args:
        router: The router object
        cbtype: 'services' or 'subscribers'
        cfg: The map containing the callback configuration,
             must include 'name' and 'content' keys
    """
    name = cfg.get("name")
    if not name:
        raise ValueError(f"add {cbtype} failed: name is required")
    content = cfg.get("content")
    if not content:
        raise ValueError(f"add {cbtype} failed: {name} impl is required")

    tag = cfg.get("tag", "main")

    # Get the broker directory and create the services directory
    dir_path = callback_dir(router.id, cbtype)
    dir_path.mkdir(parents=True, exist_ok=True)

    # Create the file path with .py extension
    file_path = dir_path / f"{name}_{tag}.py"

    logger.debug("[%s] saving callback to %s", router, file_path)

    # Remove old versions of the same service
    for old_file in dir_path.glob(f"{name}_*.py"):
        if old_file != file_path:
            old_file.unlink(missing_ok=True)

    async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
        await f.write(content)

    twin = local_twin(router)
    await eval_file(twin, cbtype, name, file_path)


async def remove_callback(router, cbtype, name):
    """
    Remove service/subscriber code to a file and dynamically load it.

    Args:
        router: The router object
        name: The service name
        content: The Python code content to save
    """
    dir_path = callback_dir(router.id, cbtype)
    if not dir_path.is_dir():
        return
    fn = dir_path / f"{name}.py"
    fn.unlink(missing_ok=True)

    twin = local_twin(router)
    if cbtype == "services":
        await twin.unexpose(name)
    elif cbtype == "subscribers":
        await twin.unsubscribe(name)


async def load_callbacks(twin):
    """Load all deployed services and subscribers callbacks."""
    router = twin.router
    for cbtype in ["services", "subscribers"]:
        dir_path = Path(broker_dir(router.id)) / "src" / cbtype
        logger.debug("[%s] loading callback from %s", twin, dir_path)

        if not dir_path.is_dir():
            return

        for path in dir_path.iterdir():
            if path.is_file() and path.suffix == ".py":
                name = path.stem
                fname = name.rsplit("_", 1)[0]
                await eval_file(twin, cbtype, fname, path)
