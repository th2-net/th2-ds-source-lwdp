from abc import ABC, abstractmethod
from typing import TYPE_CHECKING
if TYPE_CHECKING:   
    from th2_data_services_lwdp.dependencies.command import IProviderCommand, IGRPCProviderCommand

from th2_data_services.interfaces import IDataSource
from th2_data_services_lwdp.dependencies.source_api import IGRPCProviderSourceAPI, IProviderSourceAPI
from th2_data_services_lwdp.struct import IEventStruct, IMessageStruct
from th2_data_services_lwdp.stub_builder import IEventStub, IMessageStub

class IProviderDataSource(IDataSource):
    def __init__(
        self,
        url: str,
        event_struct: IEventStruct,
        message_struct: IMessageStruct,
        event_stub_builder: IEventStub,
        message_stub_builder: IMessageStub,
    ):
        """Interface of DataSource that provides work with rpt-data-provider.
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
        """str: URL of rpt-data-provider."""
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
    def command(self, cmd: 'IProviderCommand'):
        """Execute the transmitted command."""

    @property
    @abstractmethod
    def source_api(self) -> IProviderSourceAPI:
        """Returns Provider API."""


class IGRPCProviderDataSource(IProviderDataSource):
    """Interface of DataSource that provides work with rpt-data-provider via GRPC."""

    @abstractmethod
    def command(self, cmd: 'IGRPCProviderCommand'):
        """Execute the transmitted GRPC command."""

    @property
    @abstractmethod
    def source_api(self) -> IGRPCProviderSourceAPI:
        """Returns GRPC Provider API."""