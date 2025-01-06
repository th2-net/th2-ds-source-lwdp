from datetime import datetime

import pytest

from th2_data_services.data_source.lwdp.commands import http


# Commented out because behavior has changed, now it will retry calling the function so this will take a lot of time.
# def test_check_url_for_data_source():
#     with pytest.raises(requests.exceptions.ConnectionError) as exc_info:
#         data_source = DataSource("http://test_test:8080/")
#     assert "Max retries exceeded with url" in str(exc_info)


def test_command_without_end_timestamp():
    try:
        command = http.GetEventsByBookByScopes(
            book_id="demo_book_1",
            scopes=["th2-scope"],
            start_timestamp=datetime(
                year=2022, month=6, day=30, hour=14, minute=0, second=0, microsecond=0
            ),
        )
    except TypeError:
        assert False


def test_command_without_timestamps():
    with pytest.raises(TypeError):
        command = http.GetEventsByBookByScopes(
            book_id="demo_book_1",
            scopes=["th2-scope"],
        )
