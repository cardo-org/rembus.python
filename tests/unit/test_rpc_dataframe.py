import logging
import rembus
import rembus.protocol as rp
import pandas as pd

df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
df_list = [df, df]

async def echo_service(df1, df2=None):
    if df2 is None:
        return df1
    else:
        return [df1, df2]

async def test_rpc(mocker, WebSocketMockFixture):
    responses = [
        {
            # identity
            'reply': lambda req: [rp.TYPE_RESPONSE, req[1], rp.STS_OK, None]
        },
        {
            # expose 
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
        "websockets.connect",mocker.AsyncMock(return_value=WebSocketMockFixture(responses))
    )

    rb = await rembus.component('bar')
    mocked_connect.assert_called_once()
    assert mocked_connect.call_args[0][0] == "ws://127.0.0.1:8000/bar"
    await rb.expose(echo_service)

    #for payload in [df, df_list]:
    logging.info(f'[test_rpc]: {df}')
    response = await rb.rpc(echo_service.__name__, df)
    logging.info(f'response: {response}')
    assert response.equals(df)
    
    response = await rb.rpc(echo_service.__name__, *df_list)
    assert len(response) == len(df_list)
    for i in range(len(df_list)):
        assert response[i].equals(df_list[i])

    await rb.close()
