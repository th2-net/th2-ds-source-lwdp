from datetime import datetime

import pytest
import requests
from tests.tests_integration.conftest import http, HTTPDataSource


def test_check_url_for_data_source():
    with pytest.raises(requests.exceptions.ConnectionError) as exc_info:
        data_source = HTTPDataSource("http://test_test:8080/")
    assert "Max retries exceeded with url" in str(exc_info)


def test_invalid_timestamp_for_command():
    with pytest.raises(Exception, match="Provided datetime shouldn't contain microseconds"):
        command = http.GetEventsByBookByScopes(
            book_id="demo_book_1",
            scopes=["th2-scope"],
            start_timestamp=datetime(
                year=2022, month=6, day=30, hour=14, minute=0, second=0, microsecond=0
            ),
            end_timestamp=datetime.now(),
        )


def test_command_without_end_timestamp():
    try:
        command = http.GetEventsByBookByScopes(
            book_id="demo_book_1",
            scopes=["th2-scope"],
            start_timestamp=datetime(
                year=2022, month=6, day=30, hour=14, minute=0, second=0, microsecond=0
            ),
        )
    except TypeError as te:
        assert False
    except Exception as e:
        if "Provided datetime shouldn't contain microseconds" in str(e):
            assert False


def test_command_without_timestamps():
    with pytest.raises(TypeError):
        command = http.GetEventsByBookByScopes(
            book_id="demo_book_1",
            scopes=["th2-scope"],
        )
