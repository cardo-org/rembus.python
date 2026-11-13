"""Registration tests start from, and leave, a broker with no secrets."""

import pytest
import rembus

TABLES = ("tenant", "tenant_opaque", "cid_secret", "cid_opaque", "tenant_mode")


def _wipe():
    db = rembus.dbconnect()
    try:
        for table in TABLES:
            db.execute(f"DELETE FROM {table}")
    except Exception:  # pylint: disable=broad-exception-caught
        pass  # tables are created on first provisioning
    finally:
        db.close()


@pytest.fixture
def clean_secrets():
    """Mode changes are refused while old-mode secrets exist."""
    _wipe()
    yield
    _wipe()
