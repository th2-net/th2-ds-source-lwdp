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

from typing import Iterable

from sseclient import Event as SSEEvent
from urllib3.exceptions import HTTPError
import orjson as json

from th2_data_services.interfaces import IStreamAdapter
from th2_data_services_lwdp.utils.json import BufferedJSONProcessor


class SSEAdapter(IStreamAdapter):
    def __init__(self, json_processor: BufferedJSONProcessor):
        """SSE adapter. Convert SSE events to dicts.

        Args:
            json_processor: BufferedJSONProcessor
        """
        self.json_processor = json_processor
        self.events_types_blacklist = {"close", "keep_alive", "message_ids"}

    def handle(self, record: SSEEvent) -> dict:
        """Adapter handler.

        Args:
            record: SSE Event.

        Returns:
            Dict object.
        """
        if record.event == "error":
            raise HTTPError(record.data)
        if record.event not in ["close", "keep_alive", "message_ids"]:
            try:
                return json.loads(record.data)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"json.decoder.JSONDecodeError: Invalid json received.\n"
                    f"{e}\n"
                    f"{record.data}"
                )

    def handle_stream(self, stream: Iterable):
        # We need this block because we put generator function in the commands.
        # TODO this hack will be removed when we add Data.map_stream
        if callable(stream):
            stream = stream()

        for event in stream:
            if event.event == "error":
                raise HTTPError(event.data)
            if event.event not in self.events_types_blacklist:
                yield from self.json_processor.decode(event.data)
        yield from self.json_processor.fin()


def get_default_sse_adapter(buffer_limit=250):
    """Returns SSEAdapter object."""
    return SSEAdapter(BufferedJSONProcessor(buffer_limit))
