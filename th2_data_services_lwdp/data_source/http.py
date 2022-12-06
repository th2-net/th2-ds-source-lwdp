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
from __future__ import annotations

#LOG import logging

from th2_data_services.decode_error_handler import UNICODE_REPLACE_HANDLER
from typing import TYPE_CHECKING, Union

from th2_data_services.exceptions import CommandError
from th2_data_services.interfaces import IEventStruct, IMessageStruct, IEventStub, IMessageStub

if TYPE_CHECKING:
    from th2_data_services.interfaces.command import IAdaptableCommand

if TYPE_CHECKING:
    from th2_data_services_lwdp.interfaces.command import IHTTPCommand

from th2_data_services.interfaces.data_source import IDataSource
from th2_data_services_lwdp.struct import (
    http_event_struct,
    http_message_struct,
    HTTPEventStruct,
    HTTPMessageStruct,
)
from th2_data_services_lwdp.stub_builder import (
    event_stub_builder,
    message_stub_builder,
)
from th2_data_services_lwdp.source_api.http import HTTPAPI
from th2_data_services_lwdp.interfaces.data_source import IHTTPDataSource

#LOG logger = logging.getLogger(__name__)


class HTTPDataSource(IHTTPDataSource):
    """DataSource class which provide work with rpt-data-provider.

    Rpt-data-provider version: 5.x.y
    Protocol: HTTP
    """

    def __init__(
        self,
        url: str,
        chunk_length: int = 65536,
        char_enc: str = "utf-8",
        decode_error_handler: str = UNICODE_REPLACE_HANDLER,
        event_struct: IEventStruct = http_event_struct,
        message_struct: IMessageStruct = http_message_struct,
        event_stub_builder: IEventStub = event_stub_builder,
        message_stub_builder: IMessageStub = message_stub_builder,
        check_connect_timeout: Union(int, float) = 5,
    ):
        """HTTPDataSource constructor.

        Args:
            url: HTTP data source url.
            check_connect_timeout: How many seconds to wait for the server to send data before giving up.
            chunk_length: How much of the content to read in one chunk.
            char_enc: Encoding for the byte stream.
            decode_error_handler: Registered decode error handler.
            event_struct: Struct of event from rpt-data-provider.
            message_struct: Struct of message from rpt-data-provider.
            event_stub_builder: Stub for event.
            message_stub_builder: Stub for message.
        """
        super().__init__(url, event_struct, message_struct, event_stub_builder, message_stub_builder)

        self._char_enc = char_enc
        self._decode_error_handler = decode_error_handler
        self.__chunk_length = chunk_length
        self.check_connect(check_connect_timeout)
        self._provider_api = HTTPAPI(url, chunk_length, decode_error_handler, char_enc)

#LOG         logger.info(url)

    def command(self, cmd: IHTTPCommand):
        """HTTP  command processor.

        Args:
            cmd: The command of data source to execute.

        Returns:
            Data source command result.

        Raises:
            CommandError: If the command was broken.
        """
        try:
            return cmd.handle(data_source=self)
        except Exception as e:
            raise CommandError(f"The command '{cmd.__class__.__name__}' was broken. Details of error:\n{e}")

    @property
    def source_api(self) -> HTTPAPI:
        """HTTP  API."""
        return self._provider_api

    @property
    def event_struct(self) -> HTTPEventStruct:
        """Returns event structure class."""
        return self._event_struct

    @property
    def message_struct(self) -> HTTPMessageStruct:
        """Returns message structure class."""
        return self._message_struct
