#  Copyright 2022 Exactpro (Exactpro Systems Limited)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from typing import Generator, List, Union, Optional
from datetime import datetime, timezone
from functools import partial

from th2_data_services import Data
from th2_data_services.interfaces import IAdapter
from th2_data_services.exceptions import EventNotFound, MessageNotFound
from th2_data_services_lwdp.interfaces.command import IHTTPCommand
from th2_data_services_lwdp.data_source.http import HTTPDataSource
from th2_data_services_lwdp.source_api.http import HTTPAPI
from th2_data_services_lwdp.streams import Streams, Stream
from th2_data_services.sse_client import SSEClient
from th2_data_services_lwdp.adapters.adapter_sse import get_default_sse_adapter
from th2_data_services.decode_error_handler import UNICODE_REPLACE_HANDLER
from th2_data_services_lwdp.filters.event_filters import LwDPEventFilter


# LOG import logging

# LOG logger = logging.getLogger(__name__)


def _check_list_or_tuple(variable, var_name):
    if not (isinstance(variable, tuple) or isinstance(variable, list)):
        raise TypeError(f"{var_name} argument has to be list or tuple type. Got {type(variable)}")


class GetEventScopes(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It retrieves a list of event scopes in book.

    Returns:
        dict: List[str].
    """

    def __init__(self, book_id: str):
        """GetEventScopes constructor.

        Args:
            book_id: Name of book to search in.
        """
        super().__init__()
        self._book_id = book_id

    def handle(self, data_source: HTTPDataSource) -> List[str]:  # noqa: D102
        api: HTTPAPI = data_source.source_api
        url = api.get_url_get_scopes(self._book_id)

        # LOG         logger.info(url)

        return api.execute_request(url).json()


class GetMessageAliases(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It retrieves a list of message aliases in book.

    Returns:
        dict: List[str].
    """

    def __init__(self, book_id: str):
        """GetMessageAliases constructor.

        Args:
            book_id: Name of book to search in.
        """
        super().__init__()
        self._book_id = book_id

    def handle(self, data_source: HTTPDataSource) -> List[str]:  # noqa: D102
        api: HTTPAPI = data_source.source_api
        url = api.get_url_get_message_aliases(self._book_id)

        # LOG         logger.info(url)

        return api.execute_request(url).json()


class GetMessageGroups(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It retrieves a list of message groups in book.

    Returns:
        dict: List[str].
    """

    def __init__(self, book_id: str):
        """GetMessageGroups constructor.

        Args:
            book_id: Name of book to search in.
        """
        super().__init__()
        self._book_id = book_id

    def handle(self, data_source: HTTPDataSource) -> List[str]:  # noqa: D102
        api: HTTPAPI = data_source.source_api
        url = api.get_url_get_message_groups(self._book_id)

        # LOG         logger.info(url)

        return api.execute_request(url).json()


class GetBooks(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It retrieves a list of books from provider.

    Returns:
        dict: List[str].
    """

    def __init__(self):
        """GetBooks constructor."""
        super().__init__()

    def handle(self, data_source: HTTPDataSource) -> List[str]:  # noqa: D102
        api: HTTPAPI = data_source.source_api
        url = api.get_url_get_books()

        # LOG         logger.info(url)

        return api.execute_request(url).json()


class GetEventById(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It retrieves the event by id with `attachedMessageIds` list.

    Returns:
        dict: Th2 event.

    Raises:
        EventNotFound: If event by Id wasn't found.
    """

    def __init__(self, id: str, use_stub=False):
        """GetEventById constructor.

        Args:
            id: Event id.
            use_stub: If True the command returns stub instead of exception.

        """
        super().__init__()
        self._id = id
        self._stub_status = use_stub

    def handle(self, data_source: HTTPDataSource) -> dict:  # noqa: D102
        api: HTTPAPI = data_source.source_api
        url = api.get_url_find_event_by_id(self._id)

        # LOG         logger.info(url)

        response = api.execute_request(url)

        if response.status_code == 404 and self._stub_status:
            stub = data_source.event_stub_builder.build(
                {data_source.event_struct.EVENT_ID: self._id}
            )
            return stub
        elif response.status_code == 404:
            # LOG             logger.error(f"Unable to find the message. Id: {self._id}")
            raise EventNotFound(self._id)
        else:
            return response.json()


class GetEventsById(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It retrieves the events by ids with `attachedMessageIds` list.

    Returns:
        List[dict]: Th2 events.

    Raises:
        EventNotFound: If any event by Id wasn't found.
    """

    def __init__(self, ids: List[str], use_stub=False):
        """GetEventsById constructor.

        Args:
            ids: Event id list.
            use_stub: If True the command returns stub instead of exception.

        """
        super().__init__()
        self._ids: ids = ids
        self._stub_status = use_stub

    def handle(self, data_source: HTTPDataSource) -> List[dict]:  # noqa: D102
        result = []
        for event_id in self._ids:
            event = GetEventById(event_id, use_stub=self._stub_status).handle(data_source)
            result.append(event)

        return result


class GetEventsSSEBytes(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It searches events stream by options.

    Returns:
        Iterable[dict]: Stream of Th2 events.
    """

    def __init__(
        self,
        start_timestamp: datetime,
        book_id: str,
        scopes: List[str],
        end_timestamp: datetime = None,
        parent_event: str = None,
        search_direction: str = "next",
        result_count_limit: int = None,
        filters: Union[LwDPEventFilter, List[LwDPEventFilter]] = None,
    ):
        """GetEventsSSEBytes constructor.

        Args:
            Args:
            start_timestamp: Start timestamp of search.
            book_id: Book ID for messages.
            scopes: Scope names for events.
            end_timestamp: End timestamp of search.
            parent_event: Match events to the specified parent.
            search_direction: Search direction.
            result_count_limit: Result count limit.
            filters: Filters using in search for messages.

        """
        super().__init__()
        self._start_timestamp = int(1000 * start_timestamp.replace(tzinfo=timezone.utc).timestamp())
        self._end_timestamp = int(1000 * end_timestamp.replace(tzinfo=timezone.utc).timestamp())
        self._parent_event = parent_event
        self._search_direction = search_direction
        self._result_count_limit = result_count_limit
        self._filters = filters
        self._book_id = book_id
        self._scopes = scopes
        if isinstance(filters, LwDPEventFilter):
            self._filters = filters.url()
        elif isinstance(filters, (tuple, list)):
            self._filters = "".join([filter_.url() for filter_ in filters])

    def handle(self, data_source: HTTPDataSource):  # noqa: D102
        """Returns SSE Event stream in bytes."""
        api: HTTPAPI = data_source.source_api
        url = api.get_url_search_sse_events(
            start_timestamp=self._start_timestamp,
            end_timestamp=self._end_timestamp,
            parent_event=self._parent_event,
            search_direction=self._search_direction,
            result_count_limit=self._result_count_limit,
            filters=self._filters,
            book_id=self._book_id,
            scopes=self._scopes,
        )

        # LOG         logger.info(url)
        print(url)
        yield from api.execute_sse_request(url)


class GetEventsSSEEvents(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It searches events stream by options.

    Returns:
        Iterable[dict]: Stream of Th2 events.
    """

    def __init__(
        self,
        start_timestamp: datetime,
        book_id: str,
        scopes: List[str],
        end_timestamp: datetime = None,
        parent_event: str = None,
        search_direction: str = "next",
        result_count_limit: int = None,
        filters: Union[LwDPEventFilter, List[LwDPEventFilter]] = None,
        char_enc: str = "utf-8",
        decode_error_handler: str = UNICODE_REPLACE_HANDLER,
    ):
        """GetEventsSSEEvents constructor.

        Args:
            start_timestamp: Start timestamp of search.
            book_id: Book ID for messages
            scopes: Scope names for events
            end_timestamp: End timestamp of search.
            parent_event: Match events to the specified parent.
            search_direction: Search direction.
            result_count_limit: Result count limit.
            filters: Filters using in search for messages.
            char_enc: Encoding for the byte stream.
            decode_error_handler: Registered decode error handler.

        """
        super().__init__()
        self._start_timestamp = start_timestamp
        self._end_timestamp = end_timestamp
        self._parent_event = parent_event
        self._search_direction = search_direction
        self._result_count_limit = result_count_limit
        self._filters = filters
        self._book_id = book_id
        self._scopes = scopes
        self._char_enc = char_enc
        self._decode_error_handler = decode_error_handler

    def handle(self, data_source: HTTPDataSource):  # noqa: D102
        response = GetEventsSSEBytes(
            start_timestamp=self._start_timestamp,
            end_timestamp=self._end_timestamp,
            parent_event=self._parent_event,
            search_direction=self._search_direction,
            result_count_limit=self._result_count_limit,
            filters=self._filters,
            book_id=self._book_id,
            scopes=self._scopes,
        ).handle(data_source)

        client = SSEClient(
            response,
            char_enc=self._char_enc,
            decode_errors_handler=self._decode_error_handler,
        )

        yield from client.events()


class GetEvents(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It searches events stream by options.

    Returns:
        Iterable[dict]: Stream of Th2 events.
    """

    def __init__(
        self,
        start_timestamp: datetime,
        book_id: str,
        scopes: List[str],
        end_timestamp: datetime = None,
        parent_event: str = None,
        search_direction: str = "next",
        result_count_limit: int = None,
        filters: Union[LwDPEventFilter, List[LwDPEventFilter]] = None,
        cache: bool = False,
        sse_handler: Optional[IAdapter] = None,
    ):
        """GetEvents constructor.

        Args:
            start_timestamp: Start timestamp of search.
            book_id: Book ID for messages.
            scopes: Scope names for events.
            end_timestamp: End timestamp of search.
            parent_event: Match events to the specified parent.
            search_direction: Search direction.
            result_count_limit: Result count limit.
            filters: Filters using in search for messages.
            cache: If True, all requested data from lw-data-provider will be saved to cache.
            sse_handler: SSEEvents handler, by default uses StreamingSSEAdapter
        """
        super().__init__()
        self._start_timestamp = start_timestamp
        self._end_timestamp = end_timestamp
        self._parent_event = parent_event
        self._search_direction = search_direction
        self._result_count_limit = result_count_limit
        self._filters = filters
        self._book_id = book_id
        self._scopes = scopes
        self._cache = cache
        self._sse_handler = sse_handler or get_default_sse_adapter()

    def handle(self, data_source: HTTPDataSource) -> Data:  # noqa: D102
        sse_events_stream_obj = GetEventsSSEEvents(
            start_timestamp=self._start_timestamp,
            end_timestamp=self._end_timestamp,
            parent_event=self._parent_event,
            search_direction=self._search_direction,
            result_count_limit=self._result_count_limit,
            filters=self._filters,
            book_id=self._book_id,
            scopes=self._scopes,
        )

        sse_events_stream = partial(sse_events_stream_obj.handle, data_source)
        source = partial(self._sse_handler.handle_stream, sse_events_stream)

        return Data(source).use_cache(self._cache)


class GetMessageById(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It retrieves the message by id.

    Returns:
        dict: Th2 message.

    Raises:
        MessageNotFound: If message by id wasn't found.
    """

    def __init__(self, id: str, use_stub=False):
        """GetMessageById constructor.

        Args:
            id: Message id.
            use_stub: If True the command returns stub instead of exception.

        """
        super().__init__()
        self._id = id
        self._stub_status = use_stub

    def handle(self, data_source: HTTPDataSource) -> dict:  # noqa: D102
        api: HTTPAPI = data_source.source_api
        url = api.get_url_find_message_by_id(self._id)

        # LOG         logger.info(url)
        response = api.execute_request(url)

        if response.status_code == 404 and self._stub_status:
            stub = data_source.message_stub_builder.build(
                {data_source.message_struct.MESSAGE_ID: self._id}
            )
            return stub
        elif response.status_code == 404:
            # LOG             logger.error(f"Unable to find the message. Id: {self._id}")
            raise MessageNotFound(self._id)
        else:
            return response.json()


class GetMessagesById(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It retrieves the messages by ids.

    Returns:
        List[dict]: Th2 messages.

    Raises:
        MessageNotFound: If any message by id wasn't found.
    """

    def __init__(self, ids: List[str], use_stub=False):
        """GetMessagesById constructor.

        Args:
            ids: Message id list.
            use_stub: If True the command returns stub instead of exception.

        """
        super().__init__()
        self._ids: ids = ids
        self._stub_status = use_stub

    def handle(self, data_source: HTTPDataSource) -> List[dict]:  # noqa: D102
        result = []
        for message_id in self._ids:
            message = GetMessageById(
                message_id,
                use_stub=self._stub_status,
            ).handle(data_source)
            result.append(message)

        return result


class GetMessagesByStreamsSSEBytes(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It searches messages stream by options.

    Returns:
        Iterable[dict]: Stream of Th2 messages.
    """

    def __init__(
        self,
        start_timestamp: datetime,
        book_id: str,
        streams: List[Union[str, Streams]],
        message_ids: List[str] = None,
        search_direction: str = "next",
        result_count_limit: int = None,
        end_timestamp: datetime = None,
        response_formats: List[str] = None,
        keep_open: bool = False,
    ):
        """GetMessagesSSEBytes constructor.

        Args:
            start_timestamp: Start timestamp of search.
            book_id: Book ID for messages
            streams: List of aliases to request. If direction is not specified all directions will be requested for stream.
            message_ids: List of message IDs to restore search. If given, it has
                the highest priority and ignores streams (uses streams from ids), startTimestamp and resumeFromId.
            search_direction: Search direction.
            result_count_limit: Result count limit.
            end_timestamp: End timestamp of search.
            response_formats: The format of the response
            keep_open: If the search has reached the current moment.
                It is need to wait further for the appearance of new data.
        """
        super().__init__()
        self._start_timestamp = int(1000 * start_timestamp.replace(tzinfo=timezone.utc).timestamp())
        self._end_timestamp = (
            end_timestamp
            if end_timestamp is None
            else int(1000 * end_timestamp.replace(tzinfo=timezone.utc).timestamp())
        )
        self._streams = streams
        self._search_direction = search_direction
        self._result_count_limit = result_count_limit
        self._response_formats = response_formats
        self._keep_open = keep_open
        self._message_ids = message_ids
        self._book_id = book_id

    def handle(self, data_source: HTTPDataSource) -> Generator[dict, None, None]:  # noqa: D102
        api: HTTPAPI = data_source.source_api
        url = api.get_url_search_sse_messages(
            start_timestamp=self._start_timestamp,
            message_ids=self._message_ids,
            stream=[""],  # sending empty list because command handles adding streams on its own
            search_direction=self._search_direction,
            result_count_limit=self._result_count_limit,
            end_timestamp=self._end_timestamp,
            response_formats=self._response_formats,
            keep_open=self._keep_open,
            book_id=self._book_id,
        ).replace("&stream=", "")

        _check_list_or_tuple(self._streams, var_name="streams")

        if self._start_timestamp is None and not self._message_ids:
            raise TypeError("One of start_timestamp or message_id arguments must not be empty")

        fixed_part_len = len(url)
        current_url, resulting_urls = "", []
        for stream in self._streams:
            if isinstance(stream, Streams):
                stream = f"&{stream.url()}"
            elif isinstance(stream, Stream):
                stream = stream.url()
            else:
                stream = f"&stream={stream}"
            if fixed_part_len + len(current_url) + len(stream) >= 2048:
                resulting_urls.append(url + current_url)
                current_url = ""
            current_url += stream
        if current_url:
            resulting_urls.append(url + current_url)

        for url in resulting_urls:
            # LOG             logger.info(url)
            print(url)
            yield from api.execute_sse_request(url)


class GetMessagesByStreamsSSEEvents(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It searches messages stream by options.

    Returns:
        Iterable[dict]: Stream of Th2 messages.
    """

    def __init__(
        self,
        start_timestamp: datetime,
        book_id: str,
        streams: List[Union[str, Streams]],
        message_ids: List[str] = None,
        search_direction: str = "next",
        result_count_limit: int = None,
        end_timestamp: datetime = None,
        response_formats: List[str] = None,
        keep_open: bool = False,
        char_enc: str = "utf-8",
        decode_error_handler: str = UNICODE_REPLACE_HANDLER,
    ):
        """GetMessagesSSEEvents constructor.

        Args:
            start_timestamp: Start timestamp of search.
            book_id: Book ID for messages
            streams: List of aliases to request. If direction is not specified all directions will be requested for stream.
            message_ids: List of message IDs to restore search. If given, it has
                the highest priority and ignores streams (uses streams from ids), startTimestamp and resumeFromId.
            search_direction: Search direction.
            result_count_limit: Result count limit.
            end_timestamp: End timestamp of search.
            response_formats: The format of the response
            keep_open: If the search has reached the current moment.
                It is need to wait further for the appearance of new data.
            char_enc: Encoding for the byte stream.
            decode_error_handler: Registered decode error handler.
        """
        super().__init__()
        self._start_timestamp = start_timestamp
        self._end_timestamp = end_timestamp
        self._streams = streams
        self._search_direction = search_direction
        self._result_count_limit = result_count_limit
        self._response_formats = response_formats
        self._keep_open = keep_open
        self._message_ids = message_ids
        self._book_id = book_id
        self._char_enc = char_enc
        self._decode_error_handler = decode_error_handler

    def handle(self, data_source: HTTPDataSource) -> Generator[dict, None, None]:  # noqa: D102
        response = GetMessagesByStreamsSSEBytes(
            start_timestamp=self._start_timestamp,
            end_timestamp=self._end_timestamp,
            streams=self._streams,
            search_direction=self._search_direction,
            result_count_limit=self._result_count_limit,
            response_formats=self._response_formats,
            keep_open=self._keep_open,
            message_ids=self._message_ids,
            book_id=self._book_id,
        ).handle(data_source)

        client = SSEClient(
            response, char_enc=self._char_enc, decode_errors_handler=self._decode_error_handler
        )

        yield from client.events()


class GetMessagesByStreams(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It searches messages stream by options.

    Returns:
        Iterable[dict]: Stream of Th2 messages.
    """

    def __init__(
        self,
        start_timestamp: datetime,
        book_id: str,
        streams: List[Union[str, Streams]],
        message_ids: List[str] = None,
        search_direction: str = "next",
        result_count_limit: int = None,
        end_timestamp: datetime = None,
        response_formats: List[str] = None,
        keep_open: bool = False,
        char_enc: str = "utf-8",
        decode_error_handler: str = UNICODE_REPLACE_HANDLER,
        cache: bool = False,
        sse_handler: Optional[IAdapter] = None,
    ):
        """GetMessages constructor.

        Args:
            start_timestamp: Start timestamp of search.
            book_id: Book ID for messages
            streams: List of aliases to request. If direction is not specified all directions will be requested for stream.
            message_ids: List of message IDs to restore search. If given, it has
                the highest priority and ignores streams (uses streams from ids), startTimestamp and resumeFromId.
            search_direction: Search direction.
            result_count_limit: Result count limit.
            end_timestamp: End timestamp of search.
            response_formats: The format of the response
            keep_open: If the search has reached the current moment.
                It is need to wait further for the appearance of new data.
            char_enc: Encoding for the byte stream.
            decode_error_handler: Registered decode error handler.
            cache: If True, all requested data from lw-data-provider will be saved to cache.
            sse_handler: SSEEvents handler, by default uses StreamingSSEAdapter
        """
        super().__init__()
        self._start_timestamp = start_timestamp
        self._end_timestamp = end_timestamp
        self._streams = streams
        self._search_direction = search_direction
        self._result_count_limit = result_count_limit
        self._response_formats = response_formats
        self._keep_open = keep_open
        self._message_ids = message_ids
        self._book_id = book_id
        self._char_enc = char_enc
        self._decode_error_handler = decode_error_handler
        self._cache = cache
        self._sse_handler = sse_handler or get_default_sse_adapter()

    def handle(self, data_source: HTTPDataSource) -> Data:  # noqa: D102
        sse_events_stream_obj = GetMessagesByStreamsSSEEvents(
            start_timestamp=self._start_timestamp,
            end_timestamp=self._end_timestamp,
            streams=self._streams,
            search_direction=self._search_direction,
            result_count_limit=self._result_count_limit,
            response_formats=self._response_formats,
            keep_open=self._keep_open,
            message_ids=self._message_ids,
            book_id=self._book_id,
        )

        sse_events_stream = partial(sse_events_stream_obj.handle, data_source)
        source = partial(self._sse_handler.handle_stream, sse_events_stream)

        return Data(source).use_cache(self._cache)


class GetMessagesByGroupsSSEBytes(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It searches messages stream by groups.

    Returns:
        Iterable[dict]: Stream of Th2 messages.
    """

    def __init__(
        self,
        start_timestamp: datetime,
        end_timestamp: datetime,
        book_id: str,
        groups: List[str],
        sort: bool = None,
        response_formats: List[str] = None,
        keep_open: bool = None,
    ):
        """GetMessagesByGroups Constructor.

        Args:
            start_timestamp: Sets the search starting point. Expected in nanoseconds.
            end_timestamp: Sets the timestamp to which the search will be performed, starting with 'start_timestamp'.
                Expected in nanoseconds.
            book_id: book ID for requested groups.
            groups: List of groups to search messages from.
            sort: Enables message sorting within a group. It is not sorted between groups.
            response_formats: ???
            keep_open: If true, keeps pulling for new message until don't have one outside the requested range.
        """
        super().__init__()
        self._start_timestamp = int(1000 * start_timestamp.replace(tzinfo=timezone.utc).timestamp())
        self._end_timestamp = (
            end_timestamp
            if end_timestamp is None
            else int(1000 * end_timestamp.replace(tzinfo=timezone.utc).timestamp())
        )
        self._groups = groups
        self._sort = sort
        self._response_formats = response_formats
        self._keep_open = keep_open
        self._book_id = book_id

    def handle(self, data_source: HTTPDataSource) -> Generator[dict, None, None]:  # noqa: D102
        api: HTTPAPI = data_source.source_api
        url = api.get_url_search_messages_by_groups(
            start_timestamp=self._start_timestamp,
            end_timestamp=self._end_timestamp,
            groups=[],
            response_formats=self._response_formats,
            keep_open=self._keep_open,
            sort=self._sort,
            book_id=self._book_id,
        ).replace("&group=", "")

        _check_list_or_tuple(self._groups, var_name="groups")

        fixed_part_len = len(url)
        current_url, resulting_urls = "", []
        for group in self._groups:
            group = f"&group={group}"
            if fixed_part_len + len(current_url) + len(group) >= 2048:
                resulting_urls.append(url + current_url)
                current_url = ""
            current_url += group
        if current_url:
            resulting_urls.append(url + current_url)

        for url in resulting_urls:
            # LOG             logger.info(url)
            print(url)
            yield from api.execute_sse_request(url)


class GetMessagesByGroupsSSEEvents(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It searches messages strean by groups.

    Returns:
        Iterable[dict]: Stream of Th2 messages.
    """

    def __init__(
        self,
        start_timestamp: datetime,
        end_timestamp: datetime,
        book_id: str,
        groups: List[str],
        sort: bool = None,
        response_formats: List[str] = None,
        keep_open: bool = None,
        char_enc: str = "utf-8",
        decode_error_handler: str = UNICODE_REPLACE_HANDLER,
    ):
        """GetMessagesByGroupsSSEEvents Constructor.

        Args:
            start_timestamp: Sets the search starting point. Expected in nanoseconds.
            end_timestamp: Sets the timestamp to which the search will be performed, starting with 'start_timestamp'.
                Expected in nanoseconds.
            book_id: book ID for requested groups.
            groups: List of groups to search messages from.
            sort: Enables message sorting within a group. It is not sorted between groups.
            response_formats: ???
            keep_open: If true, keeps pulling for new message until don't have one outside the requested range.
            char_enc: Encoding for the byte stream.
            decode_error_handler: Registered decode error handler.
        """
        super().__init__()
        self._start_timestamp = start_timestamp
        self._end_timestamp = end_timestamp
        self._groups = groups
        self._sort = sort
        self._response_formats = response_formats
        self._keep_open = keep_open
        self._book_id = book_id
        self._char_enc = char_enc
        self._decode_error_handler = decode_error_handler

    def handle(self, data_source: HTTPDataSource) -> Generator[dict, None, None]:  # noqa: D102
        response = GetMessagesByGroupsSSEBytes(
            start_timestamp=self._start_timestamp,
            end_timestamp=self._end_timestamp,
            groups=self._groups,
            response_formats=self._response_formats,
            keep_open=self._keep_open,
            sort=self._sort,
            book_id=self._book_id,
        ).handle(data_source)

        client = SSEClient(
            response, char_enc=self._char_enc, decode_errors_handler=self._decode_error_handler
        )

        yield from client.events()


class GetMessagesByGroups(IHTTPCommand):
    """A Class-Command for request to lw-data-provider.

    It searches messages stream by groups.

    Returns:
        Iterable[dict]: Stream of Th2 messages.
    """

    def __init__(
        self,
        start_timestamp: datetime,
        end_timestamp: datetime,
        book_id: str,
        groups: List[str],
        sort: bool = None,
        response_formats: List[str] = None,
        keep_open: bool = None,
        char_enc: str = "utf-8",
        decode_error_handler: str = UNICODE_REPLACE_HANDLER,
        cache: bool = False,
        sse_handler: Optional[IAdapter] = None,
    ):
        """GetMessagesByGroups Constructor.

        Args:
            start_timestamp: Sets the search starting point. Expected in nanoseconds.
            end_timestamp: Sets the timestamp to which the search will be performed, starting with 'start_timestamp'.
                Expected in nanoseconds.
            book_id: book ID for requested groups.
            groups: List of groups to search messages from.
            sort: Enables message sorting within a group. It is not sorted between groups.
            response_formats: ???
            keep_open: If true, keeps pulling for new message until don't have one outside the requested range.
            char_enc: Encoding for the byte stream.
            decode_error_handler: Registered decode error handler.
            cache: If True, all requested data from lw-data-provider will be saved to cache.
            sse_handler: SSEEvents handler, by default uses StreamingSSEAdapter
        """
        super().__init__()
        self._start_timestamp = start_timestamp
        self._end_timestamp = end_timestamp
        self._groups = groups
        self._sort = sort
        self._response_formats = response_formats
        self._keep_open = keep_open
        self._book_id = book_id
        self._char_enc = char_enc
        self._decode_error_handler = decode_error_handler
        self._cache = cache
        self._sse_handler = sse_handler or get_default_sse_adapter()

    def handle(self, data_source: HTTPDataSource) -> Data:  # noqa: D102
        sse_events_stream_obj = GetMessagesByGroupsSSEEvents(
            start_timestamp=self._start_timestamp,
            end_timestamp=self._end_timestamp,
            groups=self._groups,
            response_formats=self._response_formats,
            keep_open=self._keep_open,
            sort=self._sort,
            book_id=self._book_id,
            char_enc=self._char_enc,
            decode_error_handler=self._decode_error_handler,
        )

        sse_events_stream = partial(sse_events_stream_obj.handle, data_source)
        source = partial(self._sse_handler.handle_stream, sse_events_stream)

        return Data(source).use_cache(self._cache)
