#  Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

from th2_data_services.interfaces import IEventStub, IMessageStub
from th2_data_services.data_source.lwdp.struct import (
    grpc_event_struct,
    grpc_message_struct,
    http_event_struct,
    http_message_struct,
    EventStruct,
    MessageStruct,
)


class BrokenEvent:
    def __str__(self):
        return "<BrokenEvent>"

    def __repr__(self):
        return "<BrokenEvent>"

    def __eq__(self, other):
        return other == BrokenEvent


class BrokenMessage:
    def __str__(self):
        return "<BrokenMessage>"

    def __repr__(self):
        return "<BrokenMessage>"

    def __eq__(self, other):
        return other == BrokenMessage


class EventStubBuilder(IEventStub):
    def __init__(self, event_struct: EventStruct):
        """Event stub builder.

        Args:
            event_struct: Event struct class.
        """
        self.event_fields = event_struct
        super().__init__()  # Requirement to define fields for the template earlier.

    @property
    def template(self) -> dict:
        """Event stub template.

        Returns:
            (dict) Event stub template.
        """
        return {
            self.event_fields.ATTACHED_MESSAGES_IDS: [],
            self.event_fields.BATCH_ID: BrokenEvent(),
            self.event_fields.END_TIMESTAMP: {"nano": 0, "epochSecond": 0},
            self.event_fields.START_TIMESTAMP: {"nano": 0, "epochSecond": 0},
            self.event_fields.EVENT_ID: self.REQUIRED_FIELD,
            self.event_fields.NAME: BrokenEvent(),
            self.event_fields.EVENT_TYPE: BrokenEvent(),
            self.event_fields.PARENT_EVENT_ID: BrokenEvent(),
            self.event_fields.STATUS: None,
            self.event_fields.IS_BATCHED: None,
        }


class MessageStubBuilder(IMessageStub):
    def __init__(self, message_struct: MessageStruct):
        """Event stub builder.

        Args:
            message_struct: Message struct class.
        """
        self.message_fields = message_struct
        super().__init__()  # Requirement to define fields for the template earlier.

    @property
    def template(self) -> dict:
        """Message stub template.

        Returns:
            (dict) Message stub template.
        """
        return {
            self.message_fields.DIRECTION: None,
            self.message_fields.SESSION_ID: BrokenMessage(),
            self.message_fields.MESSAGE_TYPE: BrokenMessage(),
            self.message_fields.TIMESTAMP: {"nano": 0, "epochSecond": 0},
            self.message_fields.BODY: [],
            self.message_fields.BODY_BASE64: [],
            self.message_fields.MESSAGE_ID: self.REQUIRED_FIELD,
            self.message_fields.ATTACHED_EVENT_IDS: [],
        }


http_event_stub_builder = EventStubBuilder(http_event_struct)
http_message_stub_builder = MessageStubBuilder(http_message_struct)
grpc_event_stub_builder = EventStubBuilder(grpc_event_struct)
grpc_message_stub_builder = MessageStubBuilder(grpc_message_struct)
