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
from json import loads
from typing import Iterable

import th2_data_services
from urllib3.exceptions import HTTPError

from th2_data_services.interfaces import IStreamAdapter
from th2_data_services_lwdp.utils.json import BufferedJSONProcessor


class SSEAdapter(IStreamAdapter):
    def __init__(self, json_processor: BufferedJSONProcessor):
        """SSE adapter. Convert SSE events to dicts.

        Args:
            json_processor: BufferedJSONProcessor
            data_link: Link to Data object, defaults to None
        """
        self.json_processor = json_processor
        self.data_link = None
        self.interactive_mode_errors = []
        # self.iterated = False
        self.events_types_blacklist = {"close", "keep_alive", "message_ids"}

    def handle(self, stream: Iterable):
        if self.data_link and th2_data_services.INTERACTIVE_MODE:
            if "errors" not in self.data_link.metadata:
                self.data_link.metadata["errors"] = []
        i = 1
        for event in stream:
            if event.event == "error":
                if th2_data_services.INTERACTIVE_MODE:
                    # if self.iterated: continue
                    self.data_link.metadata["errors"].append(loads(event.data))
                    self.interactive_mode_errors.append(loads(event.data))
                else:
                    raise HTTPError(event.data)
            if event.event not in self.events_types_blacklist:
                # if i < 10:  # !! DEBUG !!
                #     self.data_link.metadata["errors"].append([i])
                #     self.interactive_mode_errors.append([i])
                #     i += 1
                yield from self.json_processor.decode(event.data)
        # self.iterated = True
        yield from self.json_processor.fin()


DEFAULT_BUFFER_LIMIT = 250


def get_default_sse_adapter(buffer_limit=DEFAULT_BUFFER_LIMIT):
    """Returns SSEAdapter object."""
    return SSEAdapter(BufferedJSONProcessor(buffer_limit))
