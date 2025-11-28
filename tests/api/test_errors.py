"""Tests for miscellaneous error conditions."""

import os
import stat
import pytest
import cbor2
import rembus
import rembus.protocol as rp
import rembus.settings as rs


async def test_register_error(server):
    """Test the error condition that should restart the router task."""
    cid = "myname"
    # server = start_server(port=8000)
    # server = start_server_fixture
    rembus.register(cid, "11223344")

    key_dir = rs.keys_dir(server.router.id)
    curr_mode = os.stat(key_dir).st_mode
    os.chmod(key_dir, curr_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))

    try:
        myname = rembus.node(cid)
        myname.unregister()
        assert False
    except rp.RembusError:
        assert True
    finally:
        # restore permissions
        os.chmod(key_dir, curr_mode)

    myname.unregister()
    myname.close()
    # server.close()


def test_settings_error(server):
    """Test the error condition when settings file is not valid JSON."""
    fn = os.path.join(rs.rembus_dir(), rs.DEFAULT_BROKER, "settings.json")

    with open(fn, "w", encoding="utf-8") as f:
        f.write("this is not a valid json")

    with pytest.raises(RuntimeError):
        rs.Config(rs.DEFAULT_BROKER)

    os.remove(fn)


def test_unknown_message_type(server):
    """Test a reception of a Wrong payload"""
    rb = rembus.node("client")

    # Server-side throws an exception and close the connection.
    # The client detect the connection down and reconnect.
    rb._runner.run(rb._rb._send(cbor2.dumps([999])))
    rb.close()


def test_response_no_data(server):
    """Test missing data field"""
    rb = rembus.node("client")
    mid = 1234
    # This is not an error, data is set by default to None
    rb._runner.run(
        rb._rb._send(
            cbor2.dumps(
                [rp.TYPE_RESPONSE, mid.to_bytes(rp.MSGID_SZ), rp.STS_OK]
            )
        )
    )
    rb.close()
