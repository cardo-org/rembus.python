import os
import pytest
import rembus
import rembus.protocol as rp
import rembus.settings


@pytest.fixture(scope="function")
def setup_teardown():
    state = {'name': 'bar'}
    yield state
    # Teardown


async def test_register(setup_teardown, mocker, WebSocketMockFixture):
    name = setup_teardown['name']
    responses = [
        {
            # register
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            # identity
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            # rpc
        },
        {
            # response from exposed method
        }
    ]

    mocked_connect = mocker.patch(
        "websockets.connect", mocker.AsyncMock(
            return_value=WebSocketMockFixture(responses))
    )

    rembus.register(name, '11223344')
    mocked_connect.assert_called_once()
    assert mocked_connect.call_args[0][0].startswith("ws://127.0.0.1:8000")


async def test_unregister(setup_teardown, mocker, WebSocketMockFixture):
    name = setup_teardown['name']
    responses = [
        {
            # challenge
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_CHALLENGE, bytes([1, 2, 3, 4])]
        },
        {
            # attestation response
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            # unregister response
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
    ]

    mocked_connect = mocker.patch(
        "websockets.connect", mocker.AsyncMock(
            return_value=WebSocketMockFixture(responses))
    )

    rb = rembus.node(name)
    rb.unregister()
    mocked_connect.assert_called_once()

    dir = rembus.settings.rembus_dir()
    private_key = os.path.join(dir, name, '.secret')
    assert not os.path.exists(private_key)
    rb.close()
