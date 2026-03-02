import inspect
import logging
import pytest
import rembus as rb
import rembus.builtins as builtins


def service_notequal_to_topic(x, y):
    return x * y


def myservice(x, y):
    return x * y


def mytopic(x):
    pass


@pytest.mark.asyncio
async def test_spec_from_file_error():
    twin = await rb.component()
    router = twin.router
    name = "myservice"
    path = "invalid_path"

    logging.info("broker dir: %s", rb.settings.broker_dir(router.id))
    with pytest.raises(ImportError):
        await builtins.eval_file(twin, "services", name, path)

    await twin.close()


@pytest.mark.asyncio
async def test_add_myservice():
    twin = await rb.component()
    router = twin.router

    name = "myservice"
    source = inspect.getsource(myservice)

    await builtins.add_callback(
        router, "services", {"name": name, "content": source}
    )

    await twin.close()


@pytest.mark.asyncio
async def test_remove_myservice():
    twin = await rb.component()
    router = twin.router
    name = "myservice"

    # do nothing, removing unknown callback types is ignored
    await builtins.remove_callback(router, "unknown", name)

    await builtins.remove_callback(router, "services", name)

    await twin.close()


@pytest.mark.asyncio
async def test_add_mytopic():
    twin = await rb.component()
    router = twin.router

    name = "mytopic"
    source = inspect.getsource(mytopic)

    await builtins.add_callback(
        router, "subscribers", {"name": name, "content": source}
    )

    # install a tagged version of the same topic, should remove the previous one
    await builtins.add_callback(
        router, "subscribers", {"name": name, "content": source, "tag": "v2"}
    )

    lst = await builtins.list_callback(router, "subscribers")
    assert len(lst) == 1

    await twin.close()


@pytest.mark.asyncio
async def test_remove_mytopic():
    twin = await rb.component()
    router = twin.router
    name = "mytopic"

    await builtins.remove_callback(router, "subscribers", name)

    await twin.close()


@pytest.mark.asyncio
async def test_add_service_not_found():
    twin = await rb.component()
    router = twin.router

    name = "myservice"
    source = ""

    with pytest.raises(ValueError):
        await builtins.add_callback(
            router, "services", {"name": name, "content": source}
        )

    await twin.close()


@pytest.mark.asyncio
async def test_name_not_found():
    twin = await rb.component()
    router = twin.router

    source = inspect.getsource(mytopic)

    with pytest.raises(ValueError):
        await builtins.add_callback(router, "services", {"content": source})

    await twin.close()


@pytest.mark.asyncio
async def test_impl_not_found():
    twin = await rb.component()
    router = twin.router

    name = "myservice"
    source = "myvar=10"

    with pytest.raises(ValueError):
        await builtins.add_callback(
            router, "services", {"name": name, "content": source}
        )

    await twin.close()


@pytest.mark.asyncio
async def test_add_service__notequal_to_topic():
    twin = await rb.component()
    router = twin.router

    name = "service"
    source = inspect.getsource(service_notequal_to_topic)

    await builtins.add_callback(
        router, "services", {"name": name, "content": source}
    )

    await twin.close()


@pytest.mark.asyncio
async def test_list_callbacks():
    twin = await rb.component()
    router = twin.router

    lst = await builtins.list_callback(router, "subscribers")
    assert "name" in lst[0]

    lst = await builtins.list_callback(router, "subscribers", True)
    assert "body" in lst[0]

    lst = await builtins.list_callback(router, "unknow_type", True)
    assert not lst

    logging.info("list callbacks: %s", lst)
    await twin.close()
