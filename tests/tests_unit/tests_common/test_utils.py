import pytest
from datetime import datetime
from th2_data_services.data_source.lwdp.utils import _check_datetime


def test_datetime_invalid_type():
    with pytest.raises(Exception) as err:
        _check_datetime(datetime.now().timestamp())
    assert "Provided timestamp should be `datetime`, `str` or `int` object in UTC time" in str(err)
