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

# LOG import logging
from http import HTTPStatus
from typing import List, Generator, Optional, Union

import requests
from requests import Response
from urllib3 import PoolManager, exceptions
from urllib.parse import quote

from th2_data_services_lwdp.interfaces.source_api import IHTTPSourceAPI
from th2_data_services.decode_error_handler import UNICODE_REPLACE_HANDLER

# LOG logger = logging.getLogger("th2_data_services")
# LOG logger.setLevel(logging.DEBUG)


class HTTPAPI(IHTTPSourceAPI):
    def __init__(
        self,
        url: str,
        chunk_length: int = 65536,
        decode_error_handler: str = UNICODE_REPLACE_HANDLER,
        char_enc: str = "utf-8",
    ):
        """HTTP API.

        Args:
            url: HTTP data source url.
            chunk_length: How much of the content to read in one chunk.
            char_enc: Encoding for the byte stream.
            decode_error_handler: Registered decode error handler.
        """
        self._url = self.__normalize_url(url)
        self._char_enc = char_enc
        self._chunk_length = chunk_length
        self._decode_error_handler = decode_error_handler

    def __normalize_url(self, url):
        if url is None:
            return url

        pos = len(url) - 1
        while url[pos] == "/" and pos >= 0:
            pos -= 1

        return url[: pos + 1]

    def __encode_url(self, url: str) -> str:
        return quote(url.encode(), "/:&?=")

    def get_url_get_books(self) -> str:
        """REST-API `books` call returns a list of books in cradleAPI."""
        return self.__encode_url(f"{self._url}/books")

    def get_url_get_scopes(self, book_id: str) -> str:
        """REST-API `book/{bookID}/event/scopes` call returns a list of scopes in book named bookID."""
        return self.__encode_url(f"{self._url}/book/{book_id}/event/scopes")

    def get_url_get_message_aliases(self, book_id: str) -> str:
        """REST-API `book/{bookID}/message/aliases` call returns a list of message aliases in book named bookID."""
        return self.__encode_url(f"{self._url}/book/{book_id}/message/aliases")
        
    def get_url_get_message_groups(self, book_id: str) -> str:
        """REST-API `book/{bookID}/message/groups` call returns a list of message groups in book named bookID."""
        return self.__encode_url(f"{self._url}/book/{book_id}/message/groups")

    def get_url_find_event_by_id(self, event_id: str) -> str:
        """REST-API `event` call returns a single event with the specified id."""
        return self.__encode_url(f"{self._url}/event/{event_id}")

    def get_url_find_message_by_id(self, message_id: str) -> str:
        """REST-API `message` call returns a single message with the specified id."""
        return self.__encode_url(f"{self._url}/message/{message_id}")

    def get_url_search_sse_events(
        self,
        start_timestamp: int,
        book_id: str,
        scope: str,
        end_timestamp: Optional[int] = None,
        parent_event: Optional[str] = None,
        search_direction: Optional[str] = "next",
        result_count_limit: Union[int, float] = None,
        filters: Optional[str] = None,
    ) -> str:
        """REST-API `search/sse/events` call create a sse channel of event metadata that matches the filter.

        https://github.com/th2-net/th2-rpt-data-provider#sse-requests-api
        """
        kwargs = {
            "startTimestamp": start_timestamp,
            "endTimestamp": end_timestamp,
            "parentEvent": parent_event,
            "searchDirection": search_direction,
            "resultCountLimit": result_count_limit,
            "bookId": book_id,
            "scope": scope,
        }

        query = ""
        url = f"{self._url}/search/sse/events?"
        for k, v in kwargs.items():
            if v is None:
                continue
            else:
                query += f"&{k}={v}"
        url = f"{url}{query[1:]}"
        if filters is not None:
            url += filters
        return self.__encode_url(url)

    def get_url_search_sse_messages(
        self,
        start_timestamp: int,
        book_id: str,
        message_id: List[str] = None,
        stream: List[str] = None,
        search_direction: Optional[str] = "next",
        result_count_limit: Union[int, float] = None,
        end_timestamp: Optional[int] = None,
        response_formats: str = None,
        keep_open: bool = False,
    ) -> str:
        """REST-API `search/sse/messages` call create a sse channel of messages that matches the filter.

        https://github.com/th2-net/th2-rpt-data-provider#sse-requests-api
        """
        kwargs = {
            "startTimestamp": start_timestamp,
            "messageId": message_id,
            "stream": stream,
            "searchDirection": search_direction,
            "resultCountLimit": result_count_limit,
            "endTimestamp": end_timestamp,
            "responseFormats": response_formats,
            "keepOpen": keep_open,
            "bookId": book_id,
        }

        query = ""
        url = f"{self._url}/search/sse/messages?"
        for k, v in kwargs.items():
            if v is None:
                continue
            if k == "stream":
                for s in stream:
                    query += f"&{k}={s}"
            else:
                query += f"&{k}={v}"
        url = f"{url}{query[1:]}"
        return self.__encode_url(url)

    def search_message_groups(
        self, 
        start_timestamp: int, 
        end_timestamp: int, 
        book_id:str, 
        message_groups: List[str]=None, 
        sort:bool = None, 
        raw_only:bool=None,
        keep_open:bool=None
        ) -> str:
        """REST-API `search/sse/messages/group` call creates a sse channel of messages groups in specified time range.

        Args:
            start_timestamp: Sets the search starting point. Expected in nanoseconds. One of the 'start_timestamp'
                or 'resume_from_id' must not absent.
            end_timestamp: Sets the timestamp to which the search will be performed, starting with 'start_timestamp'.
                Expected in nanoseconds.
            book_id: book ID for requested groups
            message_groups: Set of books to request
            sort: Enables message sorting in the request
            raw_only: If true, only raw message will be returned in the response
            keep_open: If true, keeps pulling for new message until don't have one outside the requested range

        Returns:
            Iterable object which return messages as parts of streaming response or message stream pointers.
        """
        kwargs = {
            "startTimestamp": start_timestamp,
            "endTimestamp": end_timestamp,
            "bookId": book_id,
            "group": message_groups,
            "sort": sort,
            "onlyRaw": raw_only,
            "keepOpen": keep_open,
        }

        query = ""
        url = f"{self._url}/search/sse/messages/group?"
        for k, v in kwargs.items():
            if v is None:
                continue
            if k == "group":
                for s in message_groups:
                    query += f"&{k}={s}"
            else:
                query += f"&{k}={v}"
        url = f"{url}{query[1:]}"
        return self.__encode_url(url)

    def execute_sse_request(self, url: str) -> Generator[bytes, None, None]:
        """Create stream connection.

        Args:
            url: Url.

        Yields:
             str: Response stream data.
        """
        headers = {"Accept": "text/event-stream"}
        http = PoolManager()
        response = http.request(method="GET", url=url, headers=headers, preload_content=False)

        if response.status != HTTPStatus.OK:
            for s in HTTPStatus:
                if s == response.status:
                    raise exceptions.HTTPError(f"{s.value} {s.phrase} ({s.description}). {response.data}")
            raise exceptions.HTTPError(f"Http returned bad status: {response.status}")

        yield from response.stream(self._chunk_length)

        response.release_conn()

    def execute_request(self, url: str) -> Response:
        """Sends a GET request to provider.

        Args:
            url: Url for a get request to rpt-data-provider.

        Returns:
            requests.Response: Response data.
        """
        return requests.get(url)
