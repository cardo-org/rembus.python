import asyncio
import logging
import pytest
import traceback
import rembus as rb


async def init_component(cid):
    node = await rb.component(cid)
    await asyncio.sleep(1)
    logging.info("[test_multiple_tasks] closing %s", cid)
    await node.close()


@pytest.mark.asyncio
async def test_multiple_tasks_one_name(server):
    """Start two tasks with the same component name."""
    cid = "foo"
    task_ok = asyncio.create_task(init_component(cid))
    task_ko = asyncio.create_task(init_component(cid))

    # Wait for task_ok to complete successfully
    await task_ok

    # Wait for task_ko and expect it to raise an exception
    try:
        await task_ko
        assert False
    except Exception as e:
        assert True
        logging.info("expected error: %s", e)
        # traceback.print_exc()
