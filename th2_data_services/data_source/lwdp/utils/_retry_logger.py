#  Copyright 2024 Exactpro (Exactpro Systems Limited)
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


class RetryLogger:
    def __init__(self, name):
        """Initialize the RetryLogger.

        Args:
            name (Union[str, Callable]): The name of the function being retried.
                If a function is provided, its __name__ attribute will be used.
        """
        if callable(name):
            name = name.__name__
        self._name = name

    def warning(self, fmt, error, delay):
        """Log a warning message when a function execution fails and will be retried.

        Args:
            fmt: Format string for the warning message (unused).
            error: The error that caused the function to fail.
            delay: Number of seconds before the next retry attempt.
        """
        print(f"Function {self._name} failed with {error}. Retrying in {delay} seconds.")
