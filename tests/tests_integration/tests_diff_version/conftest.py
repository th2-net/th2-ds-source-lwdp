from collections import namedtuple
import pytest

from th2_data_services import Data
from . import (
    HTTPAPI,
    HTTPDataSource,
    Filter,
    http,
    HTTP_PORT,
)  # noqa  # noqa
from . import BOOK_NAME, SCOPE, START_TIME, END_TIME, MESSAGE_ID_1, STREAM_1, STREAM_2, all_message_bodies_http, all_event_bodies_http


@pytest.fixture
def http_data_source():
    HOST = "10.100.66.105"  # de-th2-qa
    data_source = HTTPDataSource(f"http://{HOST}:{HTTP_PORT}")
    return data_source


DataCase = namedtuple("DataCase", ["data", "expected_data_values"])


@pytest.fixture
def all_events(http_data_source: HTTPDataSource) -> DataCase:
    return DataCase(
        data=http_data_source.command(
            http.GetEventsByBookByScopes(start_timestamp=START_TIME, end_timestamp=END_TIME, book_id=BOOK_NAME,scopes=[SCOPE])
        ),
        expected_data_values=all_event_bodies_http,
    )


@pytest.fixture
def all_messages(http_data_source: HTTPDataSource) -> DataCase:
    return DataCase(
        data=http_data_source.command(
            http.GetMessagesByBookByStreams(start_timestamp=START_TIME,
                                 end_timestamp=END_TIME,
                                 streams=[STREAM_1, STREAM_2],
                                 book_id=BOOK_NAME)
        ),
        expected_data_values=all_message_bodies_http
    )


@pytest.fixture
def get_events_with_one_filter(http_data_source: HTTPDataSource) -> Data:
    case = http_data_source.command(
        http.GetEventsByBookByScopes(
            book_id=BOOK_NAME,
            scopes=[SCOPE],
            start_timestamp=START_TIME,
            end_timestamp=END_TIME,
            filters=[Filter("name", "Event for Filter test. FilterString-3")],
        )
    )

    return case


@pytest.fixture
def get_events_with_filters(http_data_source: HTTPDataSource) -> Data:
    case = http_data_source.command(
        http.GetEventsByBookByScopes(
            book_id=BOOK_NAME,
            scopes=[SCOPE],
            start_timestamp=START_TIME,
            end_timestamp=END_TIME,
            filters=[Filter("name", "Event for Filter test. FilterString-3"), Filter("type", "ds-lib-test-event")],    
        )
    )
    return case


@pytest.fixture
def events_from_data_source_with_cache_status(
    http_data_source: HTTPDataSource,
) -> Data:
    events = http_data_source.command(http.GetEventsByBookByScopes(start_timestamp=START_TIME, end_timestamp=END_TIME, book_id=BOOK_NAME,scopes=[SCOPE], cache=True))

    return events


@pytest.fixture
def messages_from_data_source_with_streams(
    http_data_source: HTTPDataSource,
) -> Data:
    messages = http_data_source.command(
        http.GetMessagesByBookByStreams(
            start_timestamp=START_TIME,
            end_timestamp=END_TIME,
            book_id=BOOK_NAME,
            streams=[
                STREAM_1,
                STREAM_2,
                "Test-123",
                "Test-1234",
                "Test-12345",
                "Test-123456",
                "Test-1234567",
                "Test-12345678",
                "Test-123456789",
                "Test-1234567810",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest1",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest2",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest3",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest4",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest5",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest6",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest7",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest8",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest9",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest10",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest11",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest12",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest13",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest14",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest15",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest16",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest17",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest18",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest19",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest20",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest21",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest22",
                "TestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTestTest23",
                "arfq01fix07",
                "arfq01dc03",
                "arfq02dc10",
            ],
        )
    )
    return messages
