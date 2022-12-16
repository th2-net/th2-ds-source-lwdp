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

from typing import Callable, Optional

from th2_data_services import Data
from th2_data_services.interfaces.events_tree import EventsTreeCollection as IETC
from th2_data_services.events_tree.exceptions import FieldIsNotExist
from th2_data_services_lwdp.data_source import GRPCDataSource
from th2_data_services_lwdp.struct import EventStruct, http_event_struct
from th2_data_services_lwdp.stub_builder import http_event_stub_builder
from th2_data_services_lwdp.command_resolver import resolver_get_events_by_id


class EventsTreeCollection(IETC):
    """EventsTreesCollections for data-provider v6."""

    def __init__(
        self,
        data: Data,
        data_source: GRPCDataSource = None,
        preserve_body: bool = False,
        # +TODO - if we will have another one protocol.
        #    We have to solve http_event_struct problem another way.
        event_struct: EventStruct = http_event_struct,
        stub: bool = False,
    ):
        """EventsTreeCollectionProvider6 constructor.

        Args:
            data: Data object.
            data_source: Data Source object.
            preserve_body: If True it will preserve 'body' field in the Events.
            event_struct: Event struct object.
            stub: If True it will create stub when event is broken.
        """
        self._event_struct = event_struct  # Should be placed before super!

        super().__init__(data=data, data_source=data_source, preserve_body=preserve_body, stub=stub)

    def _get_events_by_id_resolver(self) -> Callable:
        """Gets a function that solve which protocol command to choose."""
        return resolver_get_events_by_id

    def _get_event_id(self, event) -> str:
        """Gets event id from the event.

        Returns:
            Event id.

        Raises:
            FieldIsNotExist: If the event doesn't have an 'event id' field.
        """
        try:
            return event[self._event_struct.EVENT_ID]
        except KeyError:
            raise FieldIsNotExist(self._event_struct.EVENT_ID)

    def _get_event_name(self, event) -> str:
        """Gets event name from the event.

        Returns:
            Event name.

        Raises:
            FieldIsNotExist: If the event doesn't have an 'event name' field.
        """
        try:
            return event[self._event_struct.NAME]
        except KeyError:
            raise FieldIsNotExist(self._event_struct.NAME)

    def _get_parent_event_id(self, event) -> Optional[str]:
        """Gets parent event id from event.

        Returns:
            Parent event id.
        """
        return event.get(self._event_struct.PARENT_EVENT_ID)

    def _build_stub_event(self, id_: str):
        """Builds stub event.

        Args:
            id_: Event Id.

        Returns:
            Stub event.
        """
        return http_event_stub_builder.build({self._event_struct.EVENT_ID: id_})
