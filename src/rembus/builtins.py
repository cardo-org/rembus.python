import logging
import importlib.util
import sys
from pathlib import Path
from rembus.settings import broker_dir

logger = logging.getLogger(__name__)


def callback_dir(router_id, cbtype):
    """Return the dir where are stored services implementations"""
    return Path(broker_dir(router_id)) / "src" / cbtype


async def eval_file(twin, name, path):
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
    # (assuming it's named 'service' or same as module name)
    # Adjust this based on your naming convention
    if hasattr(mod, "service"):
        service_fn = mod.service
    elif hasattr(mod, name):
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
            raise ValueError(f"No service function found in {path}")

    await twin.expose(service_fn, name)
    return service_fn


async def add_callback(router, cbtype, name, content):
    """
    Save service/subscriber code to a file and dynamically load it.

    Args:
        router: The router object
        name: The service name
        content: The Python code content to save
    """
    # Get the broker directory and create the services directory
    dir_path = callback_dir(router.id, cbtype)
    dir_path.mkdir(parents=True, exist_ok=True)

    # Create the file path with .py extension
    file_path = dir_path / f"{name}.py"

    logger.debug("[%s] saving callback to %s", router, file_path)

    # Write the content to the file
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

    # Get the twin and evaluate the file
    twin = router.id_twin["repl"]
    await eval_file(twin, name, str(file_path))


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

    twin = router.id_twin["repl"]
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
                await eval_file(twin, name, path)
