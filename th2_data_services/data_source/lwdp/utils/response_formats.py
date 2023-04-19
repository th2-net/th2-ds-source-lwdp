#  Copyright 2023 Exactpro (Exactpro Systems Limited)
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
from typing import List, Union


class ResponseFormats:
    def __init__(self):  # noqa
        """ResponseFormats Constructor."""
        self.correct_formats = ["PROTO_PARSED", "JSON_PARSED", "BASE_64"]

    def is_valid_response_format(self, formats: Union[str, List[str]]):
        if formats is None:
            return True
        if isinstance(formats, str):
            formats = [formats]
        if not isinstance(formats, list):
            raise Exception("Wrong type. formats should be list or string")
        if "PROTO_PARSED" in self.correct_formats and "JSON_PARSED" in self.correct_formats:
            raise Exception("PROTO_PARSED and JSON_PARSED can't be used together for format")
        return all(format in self.correct_formats for format in formats)