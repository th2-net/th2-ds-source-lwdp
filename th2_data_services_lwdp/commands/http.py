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
from abc import abstractmethod
from typing import List, Optional, Union, Dict
from datetime import datetime, timezone
from functools import partial

from sseclient import Event
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


def _datetime2ms(dt_timestamp: datetime):
    """Epoch time in milliseconds."""
    return int(1000 * dt_timestamp.replace(tzinfo=timezone.utc).timestamp())


class SSEHandlerClassBase(IHTTPCommand):
    def __init__(
            self,
            sse_handler: IAdapter,
            cache: bool,
            char_enc: str = "utf-8",
            decode_error_handler: str = UNICODE_REPLACE_HANDLER,
    ):
        """TODO - add description."""
        self._current_handle_function = self._data_object
        self._char_enc = char_enc
        self._decode_error_handler = decode_error_handler
        self._sse_handler = sse_handler
        self._cache = cache

    def return_sse_bytes_stream(self):
        """TODO - add description."""
        self._current_handle_function = self._sse_bytes_stream
        return self

    def return_sse_events_stream(self):
        """TODO - add description."""
        self._current_handle_function = self._sse_events_stream
        return self

    def return_data_object(self):
        """TODO - add description."""
        self._current_handle_function = self._data_object
        return self

    @abstractmethod
    def _sse_bytes_stream(self, data_source: HTTPDataSource):
        pass

    def _sse_events_stream(self, data_source: HTTPDataSource):
        sse_bytes_stream = partial(self._sse_bytes_stream, data_source)

        client = SSEClient(
            sse_bytes_stream(),
            char_enc=self._char_enc,
            decode_errors_handler=self._decode_error_handler,
        )

        yield from client.events()

    def _data_object(self, data_source: HTTPDataSource):
        sse_events_stream = partial(self._sse_events_stream, data_source)
        source = partial(self._sse_handler.handle_stream, sse_events_stream)

        return Data(source, cache=self._cache)

    def handle(self, data_source: HTTPDataSource):
        """TODO - add description."""
        return self._current_handle_function(data_source)


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


class Page(Event):
    def __init__(self, id: int, data: Dict, event: str = 'page_info'):
        """Page Constructor.

        Args:
            id (int): Page ID
            data (Dict): Page Data
            event (str): Event Name
        """
        super().__init__(id, event, data)

    def __str__(self):
        s = ""

        if self.id:
            s += f"id: {self.id}\n"

        s += f"event: {self.event}\n"

        if self.data:
            s += f"data: {self.data}\n"
        else:
            s += "data: no data\n"

        return s


class GetPages(SSEHandlerClassBase):
    def __init__(self,
                 book_id: str,
                 start_timestamp: datetime,
                 end_timestamp: datetime,
                 sse_handler: IAdapter = get_default_sse_adapter(),
                 cache: bool = False) -> None:
        """GetPages Constructor.

        Args:
            book_id (str): Book ID
            start_timestamp (datetime): Start Timestamp
            end_timestamp (datetime): End Timestamp
            sse_handler (Optional, IAdapter): SSE Events Handler. Defaults To `SSEAdapter`
            cache (Optional, bool): Cache Status. Defaults To `False`
        """
        super().__init__(sse_handler, cache)
        self._book_id = book_id
        self._start_timestamp = start_timestamp
        self._end_timestamp = end_timestamp

    def _sse_bytes_stream(self, data_source: HTTPDataSource):  # noqa
        api: HTTPAPI = data_source.source_api
        url = api.get_url_get_pages_info(
            self._book_id,
            _datetime2ms(self._start_timestamp),
            _datetime2ms(self._end_timestamp),
        )
        # LOG             logger.info(url)
        print(url)
        yield from api.execute_sse_request(url)

    def _sse_bytes_stream_to_pages(self, data_source: HTTPDataSource):  # noqa
        source = partial(self._sse_bytes_stream, data_source)
        client = SSEClient(
            source(),
            char_enc=self._char_enc,
            decode_errors_handler=self._decode_error_handler
        )
        for event in client.events():
            print(event)
            yield Page(event.id, event.data)

    def handle(self, data_source: HTTPDataSource) -> "Data[Page]": # noqa
        source = partial(self._sse_bytes_stream_to_pages, data_source)
        return Data(source)


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


# class GetEvents(SSEHandlerClassBase):
class GetEventsByBookByScopes(SSEHandlerClassBase):
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
            end_timestamp: Optional[datetime] = None,
            parent_event: str = None,
            search_direction: str = "next",
            result_count_limit: int = None,
            filters: Union[LwDPEventFilter, List[LwDPEventFilter]] = None,
            # Non-data source args.
            # +TODO - add `max_url_length: int = 2048,`
            #   It'll be required when you implement `__split_requests` in source_api/http.py
            cache: bool = False,
            sse_handler: Optional[IAdapter] = None,
            char_enc: str = "utf-8",
            decode_error_handler: str = UNICODE_REPLACE_HANDLER,
            buffer_limit=250,
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
            char_enc: TODO - add description
            decode_error_handler: TODO - add description
            max_url_length: TODO - add description
            buffer_limit: TODO - add description
        """
        self._sse_handler = sse_handler or get_default_sse_adapter(buffer_limit=buffer_limit)
        super().__init__(
            sse_handler=self._sse_handler,
            cache=cache,
            char_enc=char_enc,
            decode_error_handler=decode_error_handler,
        )

        self._cache = cache
        # +TODO - we can make timestamps optional datetime or int. We have to check that it's in ms.

        self._start_timestamp = _datetime2ms(start_timestamp)
        self._end_timestamp = _datetime2ms(end_timestamp)
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

        _check_list_or_tuple(self._scopes, var_name="scopes")

    def _sse_bytes_stream(self, data_source):
        """Returns SSE Event stream in bytes."""
        api: HTTPAPI = data_source.source_api
        urls = [
            api.get_url_search_sse_events(
                start_timestamp=self._start_timestamp,
                end_timestamp=self._end_timestamp,
                parent_event=self._parent_event,
                search_direction=self._search_direction,
                result_count_limit=self._result_count_limit,
                filters=self._filters,
                book_id=self._book_id,
                scope=scope,
            )
            for scope in self._scopes
        ]

        # LOG         logger.info(url)
        for url in urls:
            print(url)
            yield from api.execute_sse_request(url)


class GetEventsByPageByScopes(SSEHandlerClassBase):
    # TODO - implement https://exactpro.atlassian.net/browse/TH2-4535
    pass


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


class GetMessagesByBookByStreams(SSEHandlerClassBase):
    """A Class-Command for request to lw-data-provider.

    It searches messages stream by options.

    Returns:
        Iterable[dict]: Stream of Th2 messages.
    """

    def __init__(
            self,
            start_timestamp: datetime,
            book_id: str,
            streams: Union[List[Union[str, Streams, Stream]], Streams],
            message_ids: List[str] = None,
            search_direction: str = "next",
            result_count_limit: int = None,
            end_timestamp: datetime = None,
            response_formats: List[str] = None,
            keep_open: bool = False,
            # Non-data source args.
            # +TODO - we often repeat these args. Perhaps it's better to move them to some class.
            max_url_length: int = 2048,
            char_enc: str = "utf-8",
            decode_error_handler: str = UNICODE_REPLACE_HANDLER,
            cache: bool = False,
            sse_handler: Optional[IAdapter] = None,
            buffer_limit=250,
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
                It needs to wait further for the appearance of new data.
            char_enc: Encoding for the byte stream.
            decode_error_handler: Registered decode error handler.
            cache: If True, all requested data from lw-data-provider will be saved to cache.
            sse_handler: SSEEvents handler, by default uses StreamingSSEAdapter
            max_url_length: TODO - add description
            buffer_limit: TODO - add description
        """
        self._sse_handler = sse_handler or get_default_sse_adapter(buffer_limit=buffer_limit)
        super().__init__(
            sse_handler=self._sse_handler,
            cache=cache,
            char_enc=char_enc,
            decode_error_handler=decode_error_handler,
        )

        self._streams = streams
        self._search_direction = search_direction
        self._result_count_limit = result_count_limit
        self._response_formats = response_formats
        self._keep_open = keep_open
        self._message_ids = message_ids
        self._book_id = book_id
        self._max_url_length = max_url_length
        self._char_enc = char_enc
        self._decode_error_handler = decode_error_handler
        self._cache = cache

        # + TODO - we can make timestamps optional datetime or int
        self._start_timestamp = _datetime2ms(start_timestamp)
        self._end_timestamp = (
            end_timestamp if end_timestamp is None else _datetime2ms(end_timestamp)
        )

        if isinstance(streams, Streams):
            self._streams = streams.as_list()
        elif isinstance(streams, (tuple, list, Streams)):
            self._streams = []
            for stream in streams:
                if isinstance(stream, Stream):
                    self._streams.append(stream.url()[8:])
                elif isinstance(stream, Streams):
                    self._streams += stream.as_list()
                else:
                    self._streams.append(stream)
        else:
            raise TypeError(
                f"streams argument has to be list, tuple or Streams type. "
                f"Got {type(self._streams)}"
            )

    def _sse_bytes_stream(self, data_source: HTTPDataSource):
        api: HTTPAPI = data_source.source_api
        urls = api.get_url_search_sse_messages(
            start_timestamp=self._start_timestamp,
            message_ids=self._message_ids,
            stream=self._streams,
            search_direction=self._search_direction,
            result_count_limit=self._result_count_limit,
            end_timestamp=self._end_timestamp,
            response_formats=self._response_formats,
            keep_open=self._keep_open,
            book_id=self._book_id,
            max_url_length=self._max_url_length,
        )

        if self._start_timestamp is None and not self._message_ids:
            raise TypeError("One of start_timestamp or message_id arguments must not be empty")

        for url in urls:
            # LOG             logger.info(url)
            yield from api.execute_sse_request(url)


class GetMessagesByBookByGroups(SSEHandlerClassBase):
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
            # Non-data source args.
            max_url_length: int = 2048,
            char_enc: str = "utf-8",
            decode_error_handler: str = UNICODE_REPLACE_HANDLER,
            cache: bool = False,
            sse_handler: Optional[IAdapter] = None,
            buffer_limit=250,
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
            max_url_length: TODO - add description
            buffer_limit: TODO - add description
        """
        self._sse_handler = sse_handler or get_default_sse_adapter(buffer_limit=buffer_limit)
        super().__init__(
            sse_handler=self._sse_handler,
            cache=cache,
            char_enc=char_enc,
            decode_error_handler=decode_error_handler,
        )

        self._char_enc = char_enc
        self._decode_error_handler = decode_error_handler
        self._cache = cache
        self._start_timestamp = _datetime2ms(start_timestamp)
        self._end_timestamp = (
            end_timestamp if end_timestamp is None else _datetime2ms(end_timestamp)
        )
        self._groups = groups
        self._sort = sort
        self._response_formats = response_formats
        self._keep_open = keep_open
        self._book_id = book_id
        self._max_url_length = max_url_length

        _check_list_or_tuple(self._groups, var_name="groups")

    def _sse_bytes_stream(self, data_source: HTTPDataSource):
        api: HTTPAPI = data_source.source_api
        urls = api.get_url_search_messages_by_groups(
            start_timestamp=self._start_timestamp,
            end_timestamp=self._end_timestamp,
            groups=self._groups,
            response_formats=self._response_formats,
            keep_open=self._keep_open,
            sort=self._sort,
            book_id=self._book_id,
            max_url_length=self._max_url_length,
        )

        for url in urls:
            # LOG             logger.info(url)
            yield from api.execute_sse_request(url)


class GetMessagesByPageByStreams(SSEHandlerClassBase):
    # TODO - implement https://exactpro.atlassian.net/browse/TH2-4535
    pass


class GetMessagesByPageByGroups(SSEHandlerClassBase):
    # TODO - implement https://exactpro.atlassian.net/browse/TH2-4535
    pass
