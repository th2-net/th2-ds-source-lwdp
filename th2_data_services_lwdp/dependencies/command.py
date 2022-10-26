from abc import ABC, abstractmethod
from typing import Callable
from th2_data_services_lwdp.dependencies.data_source import IProviderDataSource, IGRPCProviderDataSource
from th2_data_services.interfaces import ICommand


class IProviderCommand(ICommand):
    """Interface of command for rpt-data-provider."""

    @abstractmethod
    def handle(self, data_source: IProviderDataSource):
        pass

class IGRPCProviderCommand(ICommand):
    """Interface of command for rpt-data-provider which works via GRPC."""

    @abstractmethod
    def handle(self, data_source: IGRPCProviderDataSource):
        pass


class ProviderAdaptableCommand(IProviderCommand):
    def __init__(self):
        """Class to make Command classes adaptable."""
        self._workflow = []

    def apply_adapter(self, adapter: Callable) -> "ProviderAdaptableCommand":
        """Adds adapter to the Command workflow.
        Note, sequence that you will add adapters make sense.
        Args:
            adapter: Callable function that will be used as adapter.
        Returns:
            self
        """
        self._workflow.append(adapter)
        return self

    def _handle_adapters(self, data):
        for step in self._workflow:
            data = step(data)
        return data