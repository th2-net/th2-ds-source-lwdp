import pytest
import requests

from th2.data_services.data import Data
from th2.data_services.exceptions import CommandError
from tests.tests_integration.conftest import (
    STREAM_1,
    STREAM_2,
    http,
    HTTPDataSource,
    EVENT_ID_TEST_DATA_ROOT,
    EVENT_ID_PLAIN_EVENT_1,
    MESSAGE_ID_1,
    MESSAGE_ID_2,
)
from tests.tests_integration.test_bodies.http.event_bodies import (
    root_event_body,
    plain_event_1_body,
    filter_event_3_body,
) 
from tests.tests_integration.test_bodies.http.message_bodies import message_1_body, message_2_body


def test_issue_events(all_events):
    assert list(all_events.data) == all_events.expected_data_values


def test_issue_messages(all_messages):
    assert list(all_messages.data) == all_messages.expected_data_values

def test_find_messages_by_book_by_groups(get_messages_by_book_by_groups,all_messages):
    assert sorted(list(get_messages_by_book_by_groups),key=lambda a: a['messageId']) == sorted(all_messages.expected_data_values,key=lambda a: a['messageId']) 


def test_find_messages_by_pages_by_groups(get_messages_by_page_by_groups: Data):
    assert get_messages_by_page_by_groups.len == 28


def test_find_messages_by_pages_by_streams(get_messages_by_page_by_streams: Data):
    assert get_messages_by_page_by_streams.len == 28


def test_find_events_by_id_from_data_provider(http_data_source: HTTPDataSource):
    expected_event = root_event_body

    expected_events = [expected_event, plain_event_1_body]

    event = http_data_source.command(http.GetEventById(EVENT_ID_TEST_DATA_ROOT))
    events = http_data_source.command(
        http.GetEventsById(
            [
                EVENT_ID_TEST_DATA_ROOT,
                EVENT_ID_PLAIN_EVENT_1,
            ]
        )
    )
    events_with_one_element = http_data_source.command(
        http.GetEventsById(
            [
                EVENT_ID_TEST_DATA_ROOT,
            ]
        )
    )
    for event_ in events:
        event_["attachedMessageIds"].sort()

    broken_event: dict = http_data_source.command(http.GetEventById("id", use_stub=True))
    broken_events: list = http_data_source.command(http.GetEventsById(["id", "ids"], use_stub=True))

    plug_for_broken_event: dict = {
        "attachedMessageIds": [],
        "batchId": "Broken_Event",
        "endTimestamp": {"nano": 0, "epochSecond": 0},
        "startTimestamp": {"nano": 0, "epochSecond": 0},
        "eventId": "id",
        "eventName": "Broken_Event",
        "eventType": "Broken_Event",
        "parentEventId": "Broken_Event",
        "successful": None,
        "isBatched": None,
    }

    plug_for_broken_events: list = [
        plug_for_broken_event.copy(),
        plug_for_broken_event.copy(),
    ]
    plug_for_broken_events[1]["eventId"] = "ids"

    # Check types
    assert isinstance(event, dict)
    assert isinstance(events, list)
    assert isinstance(events_with_one_element, list)
    assert isinstance(broken_event, dict)
    assert isinstance(broken_events, list)
    # Check content.
    assert event == expected_event
    assert events == expected_events
    assert len(events) == 2
    assert len(events_with_one_element) == 1
    # Check Broken_Events
    assert broken_event == plug_for_broken_event
    assert broken_events == plug_for_broken_events
    assert [event, broken_event] == http_data_source.command(
        http.GetEventsById([EVENT_ID_TEST_DATA_ROOT, "id"], use_stub=True)
    )
    with pytest.raises(CommandError):
        http_data_source.command(http.GetEventsById([EVENT_ID_TEST_DATA_ROOT, "id"]))
    with pytest.raises(CommandError):
        http_data_source.command(http.GetEventById("id"))


def test_find_messages_by_id_from_data_provider(http_data_source: HTTPDataSource):
    expected_message = message_1_body

    expected_messages = [message_1_body, message_2_body]

    message = http_data_source.command(http.GetMessageById(MESSAGE_ID_1))
    messages = http_data_source.command(http.GetMessagesById([MESSAGE_ID_1, MESSAGE_ID_2]))
    messages_with_one_element = http_data_source.command(http.GetMessagesById([MESSAGE_ID_1]))
    # Check types
    assert isinstance(message, dict)
    assert isinstance(messages, list)
    assert isinstance(messages_with_one_element, list)
    # Check content.
    assert message == expected_message
    assert messages == expected_messages
    assert len(messages) == 2
    assert len(messages_with_one_element) == 1


def test_get_x_with_filters(
    get_events_with_one_filter: Data,
    get_events_with_filters: Data,
):
    event_case = [filter_event_3_body]
    assert list(get_events_with_one_filter) == event_case and len(event_case) is 1
    assert list(get_events_with_filters) == event_case


def test_find_message_by_id_from_data_provider_with_error(http_data_source: HTTPDataSource):
    with pytest.raises(CommandError) as exc_info:
        http_data_source.command(
            http.GetMessageById("demo-conn_not_exist:first:1624005448022245399")
        )


def test_get_events_from_data_provider_with_error(http_data_source: HTTPDataSource):
    with pytest.raises(TypeError) as exc_info:
        events = http_data_source.command(
            http.GetEventsByBookByScopes(
                start_timestamp="test", end_timestamp="test", book_id="test", scopes=["test"]
            )
        )
        list(events)
    #assert "replace() takes no keyword arguments" in str(exc_info)
    assert "Provided timestamp should be `datetime` object" in str(exc_info)


def test_get_messages_from_data_provider_with_error(http_data_source: HTTPDataSource):
    with pytest.raises(TypeError) as exc_info:
        messages = http_data_source.command(
            http.GetMessagesByBookByStreams(
                book_id="demo_book_1",
                start_timestamp="test",
                end_timestamp="test",
                streams=["test"],
            )
        )
        list(messages)
    #assert "replace() takes no keyword arguments" in str(exc_info)
    assert "Provided timestamp should be `datetime` object" in str(exc_info)


def test_check_url_for_http_data_source():
    with pytest.raises(requests.exceptions.ConnectionError) as exc_info:
        http_data_source = HTTPDataSource("http://test_test:8080/")
    assert "Max retries exceeded with url" in str(exc_info)


def test_messageIds_not_in_last_msg(all_messages: Data):
    data = all_messages
    data_lst = list(data)
    last_msg = data_lst[-1]
    assert "messageIds" not in last_msg


def test_get_messages_with_multiple_url(
    messages_from_data_source_with_streams: Data,
    all_messages: Data,
):
    messages = messages_from_data_source_with_streams.use_cache(True)

    messages_hand_expected = all_messages.data
    messages_hand_actual = messages.filter(
        lambda record: record.get("sessionId") == STREAM_1 or record.get("sessionId") == STREAM_2
    )

    assert len(list(messages)) == 6 and len(list(messages_hand_actual)) == len(
        list(messages_hand_expected)
    )#I can talk like this :D


# def test_unprintable_character(http_data_source: HTTPDataSource):
#     event = http_data_source.command(http.GetEventById(("b85d9dca-6236-11ec-bc58-1b1c943c5c0d")))
#
#     assert "\x80" in event["body"][0]["value"] and event["body"][0]["value"] == "nobJjpBJkTuQMmscc4R\x80"

"""NO ATTACHED MESSAGES ON EVENTS YET
def test_attached_messages(http_data_source: HTTPDataSource):
    events = http_data_source.command(
        http.GetEvents(
            start_timestamp=START_TIME,
            end_timestamp=END_TIME,
            attached_messages=True,
        )
    )

    assert events.filter(lambda event: event.get("attachedMessageIds")).len
"""


def test_events_for_data_loss(all_events):
    """Check that Data object won't loss its stream.

    It might happen if source inside of Data is object of generator instead of
    generator function.
    """
    events = all_events.data
    for _ in range(3):
        for _ in events:
            pass
    assert list(events) == all_events.expected_data_values


def test_messages_for_data_loss(all_messages):
    """Check that Data object won't loss its stream.

    It might happen if source inside of Data is object of generator instead of
    generator function.
    """
    
    messages = all_messages.data
    for _ in range(3):
        for _ in messages:
            pass

    assert list(messages) == all_messages.expected_data_values
