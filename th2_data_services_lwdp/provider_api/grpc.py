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

from email import message
import logging
from collections import namedtuple
from typing import Iterable, List, Optional, Union

from google.protobuf.empty_pb2 import Empty
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import BoolValue, Int32Value, Int64Value
from th2_grpc_common.common_pb2 import MessageID, EventID, ConnectionID, Direction
from th2_grpc_data_provider.data_provider_pb2_grpc import DataProviderStub
from th2_grpc_data_provider.data_provider_pb2 import (
    EventResponse,
    MessageGroupResponse,
    MessageStreamsResponse,
    MessageSearchResponse,
    Filter,
    EventSearchResponse,
    FilterNamesResponse,
    FilterInfoResponse,
    FilterName,
    MatchResponse,
    EventMatchRequest,
    MessageMatchRequest,
    EventSearchRequest,
    MessageSearchRequest,
    TimeRelation,
    MessageStream,
    MessageStreamPointer,
)
from grpc import Channel, insecure_channel
from th2_data_services.provider.interfaces.source_api import IGRPCProviderSourceAPI
from th2_data_services_lwdp.streams import Streams

logger = logging.getLogger(__name__)

BasicRequest = namedtuple(
    "BasicRequest",
    ["start_timestamp", "end_timestamp", "result_count_limit", "search_direction"],
)


class GRPCAPI(IGRPCProviderSourceAPI):
    def __init__(self, url: str):
        """GRPC API.

        Args:
            url: GRPC data source url.
        """
        self._create_connection(url)

    def _create_connection(self, url: str) -> None:
        """Creates gRPC channel to gRPC-server.

        Args:
            url: Url of gRPC-server.
        """
        channel: Channel = insecure_channel(url)
        self.__stub: DataProviderStub = DataProviderStub(channel)

    def get_message_streams(self) -> MessageStreamsResponse:
        """GRPC-API `getMessageStreams` call returns a list of message stream names."""
        return self.__stub.getMessageStreams(Empty())

    def search_events(
        self,
        start_timestamp: int = None,
        end_timestamp: int = None,
        parent_event: str = None
    ) -> Iterable[EventSearchResponse]:
        """GRPC-API `searchEvents` call creates an event or an event metadata stream that matches the filter.

        Args:
            start_timestamp: Sets the search starting point. Expected in nanoseconds. One of the 'start_timestamp'
                or 'resume_from_id' must not absent.
            end_timestamp: Sets the timestamp to which the search will be performed, starting with 'start_timestamp'.
                Expected in nanoseconds.
            parent_event: Match events to the specified parent.

        Returns:
            Iterable object which return events as parts of streaming response.
        """
        self.__search_basic_checks(
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )

        basic_request = self.__build_basic_request_object(
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )
        parent_event = EventID(id=parent_event) if parent_event else None

        event_search_request = EventSearchRequest(
            start_timestamp=basic_request.start_timestamp,
            end_timestamp=basic_request.end_timestamp,
            parent_event=parent_event
        )
        return self.__stub.searchEvents(event_search_request)

    def search_messages(
        self,
        stream: List[str],
        start_timestamp: int = None,
        end_timestamp: int = None,
        search_direction: str = "NEXT",
        result_count_limit: int = None,
        stream_pointer: List[MessageStreamPointer] = None,
    ) -> Iterable[MessageSearchResponse]:
        """GRPC-API `searchMessages` call creates a message stream that matches the filter.

        Args:
            start_timestamp: Sets the search starting point. Expected in nanoseconds. One of the 'start_timestamp'
                or 'resume_from_id' must not absent.
            stream: Sets the stream ids to search in.
            end_timestamp: Sets the timestamp to which the search will be performed, starting with 'start_timestamp'.
                Expected in nanoseconds.
            search_direction: Sets the lookup direction. Can be 'NEXT' or 'PREVIOUS'.
            result_count_limit: Sets the maximum amount of messages to return.
            stream_pointer: List of stream pointers to restore the search from.
                start_timestamp will be ignored if this parameter is specified. This parameter is only received
                from the provider.

        Returns:
            Iterable object which return messages as parts of streaming response or message stream pointers.
        """
        if stream is None:
            raise TypeError("Argument 'stream' is required.")
        self.__search_basic_checks(
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            result_count_limit=result_count_limit,
            search_direction=search_direction,
        )

        basic_request = self.__build_basic_request_object(
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            result_count_limit=result_count_limit,
            search_direction=search_direction,
        )

        stream = self.__transform_streams(stream)
        message_search_request = MessageSearchRequest(
            start_timestamp=basic_request.start_timestamp,
            end_timestamp=basic_request.end_timestamp,
            search_direction=basic_request.search_direction,
            result_count_limit=basic_request.result_count_limit,
            stream_pointer=stream_pointer,
            stream=stream,
        )

        return self.__stub.searchMessages(message_search_request)

    @staticmethod
    def __search_basic_checks(
        start_timestamp: Optional[int],
        end_timestamp: Optional[int],
        search_direction: Optional[str],
        result_count_limit: Optional[int],
    ):
        if start_timestamp is None:
            raise ValueError("One of the 'startTimestamp' or 'resumeFromId(s)' must not be None.")

        if end_timestamp is None and result_count_limit is None:
            raise ValueError("One of the 'end_timestamp' or 'result_count_limit' must not be None.")

        if (
            start_timestamp is not None
            and len(str(start_timestamp)) != 19
            or end_timestamp is not None
            and len(str(end_timestamp)) != 19
        ):
            raise ValueError("Arguments 'start_timestamp' and 'end_timestamp' are expected in nanoseconds.")

        if search_direction is not None:
            search_direction = search_direction.upper()
            if search_direction not in ("NEXT", "PREVIOUS"):
                raise ValueError("Argument 'search_direction' must be 'NEXT' or 'PREVIOUS'.")
        else:
            raise ValueError("Argument 'search_direction' must be 'NEXT' or 'PREVIOUS'.")


    def __transform_streams(self, streams: List[str]) -> List[MessageStream]:
        """Transforms streams to MessagesStream of 'protobuf' entity.

        Args:
            streams: Streams.

        Returns:
            List of MessageStream.
        """
        new_streams = []
        for raw_stream in streams:
            if isinstance(raw_stream, Streams):
                msg_stream = raw_stream.grpc()
            else:
                msg_stream = self.__build_message_stream(raw_stream)
            if isinstance(msg_stream, list):
                new_streams.extend(msg_stream)
            else:
                new_streams.append(msg_stream)
        return new_streams

    @staticmethod
    def __build_message_stream(raw_stream: str) -> Union[MessageStream, List[MessageStream]]:
        """Builds MessageStream of 'protobuf' entity.

        Note that if raw_stream doesn't have direction, the function returns both directions.

        Args:
            raw_stream: Stream string as 'alias:direction'

        Returns:
            MessageStream.
        """
        splitted_stream = raw_stream.split(":")
        name = splitted_stream[0]
        if len(splitted_stream) > 1:
            name, search_direction = ":".join(splitted_stream[0:-1]), splitted_stream[-1].upper()
            if search_direction in ("FIRST", "SECOND"):
                return MessageStream(name=name, direction=Direction.Value(search_direction))
            else:
                name = ":".join(splitted_stream)
        return [
            MessageStream(name=name, direction=Direction.Value("FIRST")),
            MessageStream(name=name, direction=Direction.Value("SECOND")),
        ]

    def __build_basic_request_object(
        self,
        start_timestamp: int = None,
        end_timestamp: int = None,
        result_count_limit: int = None,
        search_direction: str = "NEXT"
    ) -> BasicRequest:
        """Builds a BasicRequest wrapper-object.
        Args:
            start_timestamp: Start Timestamp for request.
            end_timestamp: End Timestamp for request.
            result_count_limit: Data count limit.
            keep_open: Option for stream-request.
            search_direction: Searching direction.
            filters: Which filters to apply in a request.
        Returns:
            BasicRequest wrapper-object.
        """

        start_timestamp = self.__build_timestamp_object(start_timestamp) if start_timestamp else None
        end_timestamp = self.__build_timestamp_object(end_timestamp) if end_timestamp else None
        search_direction = TimeRelation.Value(search_direction)  # getting a value from enum
        result_count_limit = Int32Value(value=result_count_limit) if result_count_limit else None
        

        basic_request = BasicRequest(
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            search_direction=search_direction,
            result_count_limit=result_count_limit
        )
        return basic_request

    @staticmethod
    def __build_timestamp_object(timestamp: int) -> Timestamp:
        """Builds a Timestamp of 'protobuf' entity.

        Args:
            timestamp: Timestamp in nanoseconds.

        Returns:
            Timestamp object.
        """
        nanos = timestamp % 10 ** 9
        seconds = timestamp // 10 ** 9
        timestamp = Timestamp(seconds=seconds, nanos=nanos)
        return timestamp

    @staticmethod
    def __build_message_id_object(message_id: str) -> MessageID:
        """Builds a MessageID of 'protobuf' entity.

        Args:
            message_id: Message id.

        Returns:
            MessageID object.
        """
        split_id = message_id.split(":")
        split_id.reverse()  # Parse the message id from the end.

        sequence, direction, alias = split_id[0], split_id[1].upper(), split_id[2:]
        split_sequence = sequence.split(".")
        sequence, subsequence = int(split_sequence[0]), split_sequence[1:]
        subsequence = map(int, subsequence) if subsequence else subsequence
        alias.reverse()
        alias = ":".join(alias)

        connection_id = ConnectionID(session_alias=alias)
        direction = Direction.Value(direction)  # Get a value from enum.

        message_id = MessageID(
            connection_id=connection_id,
            direction=direction,
            sequence=sequence,
            subsequence=subsequence,
        )
        return message_id

    def get_event(self, event_id: str) -> EventResponse:
        """GRPC-API `getEvent` call returns a single event with the specified id."""
        event_id = EventID(id=event_id)
        return self.__stub.getEvent(event_id)

    def get_message(self, message_id: str) -> MessageGroupResponse:
        """GRPC-API `getMessage` call returns a single message with the specified id."""
        message_id = self.__build_message_id_object(message_id)
        return self.__stub.getMessage(message_id)