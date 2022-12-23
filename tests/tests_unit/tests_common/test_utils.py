import pytest
from datetime import datetime
from th2_data_services_lwdp.utils import _check_millisecond


def test_milliseconds():
    assert _check_millisecond(datetime.now().timestamp() * 1_000)


def test_invalid_milliseconds():
    with pytest.raises(Exception) as err:
        _check_millisecond(datetime.now().timestamp() * 10_000)
    assert "Provided Timestamp Is Not In Milliseconds" in str(err)
