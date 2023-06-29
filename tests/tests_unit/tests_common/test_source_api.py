from datetime import datetime

from th2_data_services.data_source.lwdp.filters.filter import Filter 
from th2_data_services.data_source.lwdp.source_api import API


def test_generate_url_search_sse_events():
    api = API(url="http://host:port")

    start_time = int(datetime.now().timestamp() * 1000)
    end_time = int(datetime.now().timestamp() * 1000)
    parent_event_id = "event_test"
    book = "testbook"
    scope = "th2-scope"

    url = api.get_url_search_sse_events(
        start_timestamp=start_time,
        end_timestamp=end_time,
        book_id=book,
        scope=scope,
        parent_event=parent_event_id,
    )

    assert (
        url
        == f"http://host:port/search/sse/events?startTimestamp={start_time}&endTimestamp={end_time}"
        f"&parentEvent={parent_event_id}&searchDirection=next&bookId={book}&scope={scope}"
    )


def test_generate_url_search_sse_events_with_filters():
    api = API(url="http://host:port")

    start_time = int(datetime.now().timestamp() * 1000)
    end_time = int(datetime.now().timestamp() * 1000)
    filter_name, filter_value = "name", "test"
    book = "testbook"
    scope = "th2-scope"

    url = api.get_url_search_sse_events(
        start_timestamp=start_time,
        end_timestamp=end_time,
        book_id=book,
        scope=scope,
        filters=Filter(filter_name, filter_value).url(),
    )

    assert (
        url
        == f"http://host:port/search/sse/events?startTimestamp={start_time}&endTimestamp={end_time}"
        f"&searchDirection=next&bookId={book}&scope={scope}&filters={filter_name}"
        f"&{filter_name}-values={filter_value}&{filter_name}-negative=False&{filter_name}-conjunct=False"
    )


def test_generate_url_search_sse_messages():
    api = API(url="http://host:port")

    start_time = int(datetime.now().timestamp() * 1000)
    end_time = int(datetime.now().timestamp() * 1000)
    stream = ["test-stream", "test-stream2"]
    book = "testbook"

    url = api.get_url_search_sse_messages(
        start_timestamp=start_time,
        end_timestamp=end_time,
        keep_open=True,
        stream=stream,
        book_id=book,
    )
    assert (
        url[0]
        == f"http://host:port/search/sse/messages?startTimestamp={start_time}&searchDirection=next&endTimestamp={end_time}"
        f"&keepOpen={True}&bookId={book}&stream={stream[0]}&stream={stream[1]}"
    )


def test_generate_url_search_sse_messages_by_groups():
    api = API(url="http://host:port")

    start_time = int(datetime.now().timestamp() * 1000)
    end_time = int(datetime.now().timestamp() * 1000)
    book = "testbook"
    groups = ["testgroup1", "testgroup2"]

    url = api.get_url_search_messages_by_groups(
        start_timestamp=start_time,
        end_timestamp=end_time,
        keep_open=True,
        book_id=book,
        groups=groups,
    )

    assert (
        url[0]
        == f"http://host:port/search/sse/messages/group?startTimestamp={start_time}&endTimestamp={end_time}&bookId={book}"
        f"&keepOpen={True}&group={groups[0]}&group={groups[1]}"
    )


def test_long_url_splitting():
    api = API(url="http://host:port")

    start_time = int(datetime.now().timestamp() * 1000)
    end_time = int(datetime.now().timestamp() * 1000)
    stream = [
        "Test-1234",
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
        "arfq02fix30",
    ]
    book = "testbook"

    urls = api.get_url_search_sse_messages(
        start_timestamp=start_time,
        end_timestamp=end_time,
        keep_open=True,
        stream=stream,
        book_id=book,
    )
    assert len(urls) > 1
    for url in urls:
        assert len(url) < 2048


def test_encoding_url():
    api = API(url="http://host:port")

    start_time = int(datetime.now().timestamp() * 1000)
    end_time = int(datetime.now().timestamp() * 1000)
    filter_name, filter_value = "test0 test1", "test2 test3"
    book = "testbook"
    scope = "th2-scope"

    url = api.get_url_search_sse_events(
        start_timestamp=start_time,
        end_timestamp=end_time,
        book_id=book,
        scope=scope,
        filters=Filter(filter_name, filter_value).url(),
    )

    filter_name = filter_name.split()
    filter_value = filter_value.split()

    encoded_filter_name = filter_name[0] + "%20" + filter_name[1]

    assert (
        url
        == f"http://host:port/search/sse/events?startTimestamp={start_time}&endTimestamp={end_time}"
        f"&searchDirection=next&bookId={book}&scope={scope}&filters={encoded_filter_name}"
        f"&{encoded_filter_name}-values={filter_value[0] + '%20' + filter_value[1]}&"
        f"{encoded_filter_name}-negative=False&{encoded_filter_name}-conjunct=False"
    )


def test_count_slash_in_non_standart_url():
    api0 = API(url="http://host:port")
    api1 = API(url="http://host:port/")
    api4 = API(url="http://host:port/////")
    api10 = API(url="//////////")

    assert (
        api0._url == "http://host:port"
        and api1._url == "http://host:port"
        and api4._url == "http://host:port"
        and api10._url == ""
    )
