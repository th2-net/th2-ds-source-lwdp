import pytest
from datetime import datetime
from th2.data_services.lwdp.utils import _check_milliseconds


def test_milliseconds():
    assert _check_milliseconds(datetime.now().replace(microsecond=0)) is None


def test_milliseconds_invalid_type():
    with pytest.raises(Exception) as err:
        _check_milliseconds(datetime.now().timestamp())
    assert "Provided timestamp should be `datetime` object" in str(err)


def test_milliseconds_invalid_time():
    with pytest.raises(Exception) as err:
        _check_milliseconds(datetime.now())
    assert "Provided datetime shouldn't contain microseconds" in str(err)
