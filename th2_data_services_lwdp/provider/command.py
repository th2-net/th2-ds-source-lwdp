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
from typing import Callable
from th2_data_services_lwdp.provider.data_source import IProviderDataSource, IGRPCProviderDataSource
from th2_data_services.interfaces import ICommand


class IProviderCommand(ICommand):
    """Interface of command for lwdp-data-provider."""

    @abstractmethod
    def handle(self, data_source: IProviderDataSource):
        pass

class IGRPCProviderCommand(ICommand):
    """Interface of command for lwdp-data-provider which works via GRPC."""

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