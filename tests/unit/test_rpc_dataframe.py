"""Tests for Dataframes arguments."""
import narwhals as nw
import pandas as pd
import rembus
import rembus.protocol as rp

df = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
df_list = [df, df]


async def echo_service(df1, df2=None):
    """A service that echoes the received DataFrame(s)."""
    if df2 is None:
        return df1
    else:
        return [df1, df2]


async def test_rpc(mocker, ws_mock):
    """Test the RPC method of the rembus component with DataFrame arguments."""
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
        "websockets.connect", mocker.AsyncMock(
            return_value=ws_mock(responses))
    )

    rb = await rembus.component('bar')
    mocked_connect.assert_called_once()
    assert mocked_connect.call_args[0][0] == "ws://127.0.0.1:8000/bar"
    await rb.expose(echo_service)

    # for payload in [df, df_list]:
    response = await rb.rpc(echo_service.__name__, df)
    res_df = nw.from_native(response)
    src_df = nw.from_native(df)
    assert src_df.to_pandas().equals(res_df.to_pandas())

    response = await rb.rpc(echo_service.__name__, *df_list)
    assert len(response) == len(df_list)
    for i, _ in enumerate(df_list):
        res_df = nw.from_native(response[i])
        src_df = nw.from_native(df_list[i])
        assert src_df.to_pandas().equals(res_df.to_pandas())

    await rb.close()
