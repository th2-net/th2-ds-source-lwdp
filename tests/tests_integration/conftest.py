from collections import namedtuple
from th2_data_services.data import Data
from datetime import datetime
import pytest

from tests.tests_integration.test_bodies.http.all_test_event_bodies import all_event_bodies_http
from tests.tests_integration.test_bodies.http.all_test_message_bodies import all_message_bodies_http
from th2_data_services.data_source.lwdp.data_source import HTTPDataSource
from th2_data_services.data_source.lwdp.filters import NameFilter, TypeFilter

STREAM_1 = "ds-lib-session1"
STREAM_2 = "ds-lib-session2"

BOOK_NAME = "demo_book_1"
SCOPE = "th2-scope"

from th2_data_services.data_source.lwdp.commands import http  # noqa
from th2_data_services.data_source.lwdp.source_api import HTTPAPI  # noqa

START_TIME = datetime(year=2023, month=1, day=5, hour=13, minute=57, second=5, microsecond=0)
END_TIME = datetime(year=2023, month=1, day=5, hour=13, minute=57, second=6, microsecond=0)

EVENT_ID_TEST_DATA_ROOT = (
    "demo_book_1:th2-scope:20230105135705560873000:d61e930a-8d00-11ed-aa1a-d34a6155152d_1"
)
EVENT_ID_PLAIN_EVENT_1 = "demo_book_1:th2-scope:20230105135705563522000:9adbb3e0-5f8b-4c28-a2ac-7361e8fa704c>demo_book_1:th2-scope:20230105135705563522000:d61e930a-8d00-11ed-aa1a-d34a6155152d_2"
EVENT_ID_PLAIN_EVENT_2 = "demo_book_1:th2-scope:20230105135705563522000:9adbb3e0-5f8b-4c28-a2ac-7361e8fa704c>demo_book_1:th2-scope:20230105135705563757000:d61e930a-8d00-11ed-aa1a-d34a6155152d_3"

MESSAGE_ID_1 = "demo_book_1:ds-lib-session1:1:20230105135705559347000:1672927025546507570"
MESSAGE_ID_2 = "demo_book_1:ds-lib-session1:1:20230105135705559529000:1672927025546507571"

HTTP_PORT = "32681"  # HTTP lwdp


@pytest.fixture
def http_data_source() -> HTTPDataSource:
    HOST = "10.100.66.105"  # de-th2-qa
    data_source = HTTPDataSource(f"http://{HOST}:{HTTP_PORT}")
    return data_source


DataCase = namedtuple("DataCase", ["data", "expected_data_values"])


@pytest.fixture
def all_events(http_data_source: HTTPDataSource) -> DataCase:
    return DataCase(
        data=http_data_source.command(
            http.GetEventsByBookByScopes(
                start_timestamp=START_TIME,
                end_timestamp=END_TIME,
                book_id=BOOK_NAME,
                scopes=[SCOPE],
            )
        ),
        expected_data_values=all_event_bodies_http,
    )


@pytest.fixture
def all_messages(http_data_source: HTTPDataSource) -> DataCase:
    comm = http.GetMessagesByBookByStreams(
        start_timestamp=START_TIME,
        end_timestamp=END_TIME,
        streams=[STREAM_1, STREAM_2],
        book_id=BOOK_NAME,
    )
    case = DataCase(
        data=http_data_source.command(comm),
        expected_data_values=all_message_bodies_http,
    )
    return case


@pytest.fixture
def get_messages_by_page_by_streams(http_data_source: HTTPDataSource) -> Data:
    pages = http_data_source.command(
        http.GetPages("demo_book_1", start_timestamp=START_TIME, end_timestamp=END_TIME)
    )

    messages = http_data_source.command(
        http.GetMessagesByPageByStreams(
            page=list(pages)[0], stream=["ds-lib-session1", "ds-lib-session2"]
        )
    )

    return messages


@pytest.fixture
def get_messages_by_book_by_groups(http_data_source: HTTPDataSource) -> Data:
    messages = http_data_source.command(
        http.GetMessagesByBookByGroups(
            start_timestamp=START_TIME,
            end_timestamp=END_TIME,
            groups=["ds-lib-session1", "ds-lib-session2"],
            book_id=BOOK_NAME,
        )
    )

    return messages


@pytest.fixture
def get_messages_by_page_by_groups(http_data_source: HTTPDataSource) -> Data:
    pages = http_data_source.command(
        http.GetPages("demo_book_1", start_timestamp=START_TIME, end_timestamp=END_TIME)
    )

    messages = http_data_source.command(
        http.GetMessagesByPageByGroups(
            page=list(pages)[0], groups=["ds-lib-session1", "ds-lib-session2"]
        )
    )

    return messages


@pytest.fixture
def get_events_with_one_filter(http_data_source: HTTPDataSource) -> Data:
    case = http_data_source.command(
        http.GetEventsByBookByScopes(
            book_id=BOOK_NAME,
            scopes=[SCOPE],
            start_timestamp=START_TIME,
            end_timestamp=END_TIME,
            filters=[NameFilter("Event for Filter test. FilterString-3")],
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
            filters=[
                NameFilter("Event for Filter test. FilterString-3"),
                TypeFilter("ds-lib-test-event"),
            ],
        )
    )
    return case


@pytest.fixture
def events_from_data_source_with_cache_status(
    http_data_source: HTTPDataSource,
) -> Data:
    events = http_data_source.command(
        http.GetEventsByBookByScopes(
            start_timestamp=START_TIME,
            end_timestamp=END_TIME,
            book_id=BOOK_NAME,
            scopes=[SCOPE],
            cache=True,
        )
    )

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
