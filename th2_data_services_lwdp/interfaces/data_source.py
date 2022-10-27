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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from th2_data_services_lwdp.interfaces.command import IGRPCCommand, ILwDPCommand

from th2_data_services.interfaces import IDataSource
from th2_data_services_lwdp.interfaces.source_api import IGRPCSourceAPI, ILwDPSourceAPI
from th2_data_services_lwdp.struct import IEventStruct, IMessageStruct
from th2_data_services_lwdp.stub_builder import IEventStub, IMessageStub


class ILwDPDataSource(IDataSource):
    def __init__(
            self,
            url: str,
            event_struct: IEventStruct,
            message_struct: IMessageStruct,
            event_stub_builder: IEventStub,
            message_stub_builder: IMessageStub,
    ):
        """Interface of DataSource that provides work with lwdp-data-provider.
        Args:
            url: Url address to data provider.
            event_struct: Event struct class.
            message_struct: Message struct class.
            event_stub_builder: Event stub builder class.
            message_stub_builder: Message stub builder class.
        """
        if url[-1] == "/":
            url = url[:-1]
        self._url = url
        self._event_struct = event_struct
        self._message_struct = message_struct
        self._event_stub_builder = event_stub_builder
        self._message_stub_builder = message_stub_builder

    @property
    def url(self) -> str:
        """str: URL of lwdp-data-provider."""
        return self._url

    @property
    def event_struct(self) -> IEventStruct:
        """Returns event structure class."""
        return self._event_struct

    @property
    def message_struct(self) -> IMessageStruct:
        """Returns message structure class."""
        return self._message_struct

    @property
    def event_stub_builder(self) -> IEventStub:
        """Returns event stub template."""
        return self._event_stub_builder

    @property
    def message_stub_builder(self) -> IMessageStub:
        """Returns message stub template."""
        return self._message_stub_builder

    @abstractmethod
    def command(self, cmd: 'ILwDPCommand'):
        """Execute the transmitted command."""

    @property
    @abstractmethod
    def source_api(self) -> ILwDPSourceAPI:
        """Returns Provider API."""


class IGRPCDataSource(ILwDPDataSource):
    """Interface of DataSource that provides work with lwdp-data-provider via GRPC."""

    @abstractmethod
    def command(self, cmd: 'IGRPCCommand'):
        """Execute the transmitted GRPC command."""

    @property
    @abstractmethod
    def source_api(self) -> IGRPCSourceAPI:
        """Returns GRPC Provider API."""
