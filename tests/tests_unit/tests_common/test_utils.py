import pytest
from datetime import datetime

from th2_data_services.data_source.lwdp import Stream, Streams
from th2_data_services.data_source.lwdp.streams import _convert_stream_to_dict_format
from th2_data_services.data_source.lwdp.utils import _check_timestamp


def test_datetime_invalid_type():
    with pytest.raises(Exception) as err:
        _check_timestamp(datetime.now().timestamp())
    assert "Provided timestamp should be `datetime`, `str` or `int` object in UTC time" in str(err)


def test_convert_stream_to_dict_format():
    streams = [
        "s1:1",
        {"sessionAlias": "s2", "direction": ["IN"]},
        Stream("s3", 2),
        Streams(["s4"]),
    ]
    expected = [
        {"sessionAlias": "s1", "direction": ["IN"]},
        {"sessionAlias": "s2", "direction": ["IN"]},
        {"sessionAlias": "s3", "direction": ["OUT"]},
        {"sessionAlias": "s4"},
    ]
    assert _convert_stream_to_dict_format(streams) == expected
