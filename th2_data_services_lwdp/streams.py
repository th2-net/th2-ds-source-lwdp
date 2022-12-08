from typing import List

from th2_grpc_common.common_pb2 import Direction
from th2_grpc_data_provider.data_provider_pb2 import MessageStream


# TODO - perhaps it's a good idea to create class Stream
#   It'll look like Stream('abc', 1)
#   or in the future Stream(alias='abc', direction=1, book_id='B1')

class Streams:
    """General interface for composite streams of Provider v6.

    The class gives the opportunity to make list of streams with direction for each.
    """

    def __init__(self, aliases: List[str], direction: str = None):
        """Streams constructor.

        Args:
            aliases: List of Streams.
            direction: Direction of Streams (Only FIRST or SECOND). If None then is both directions.
        """
        # TODO - direction is 1 or 2 in HTTP!
        self._aliases = aliases
        if direction is not None:
            direction = direction.upper()
            if direction not in ("FIRST", "SECOND"):
                raise ValueError("The direction must be 'FIRST' or 'SECOND'.")
        self._direction = direction

    def __repr__(self):
        class_name = self.__class__.__name__
        return f"{class_name}(" \
               f"aliases={self._aliases}, " \
               f"direction={self._direction})"

    def url(self) -> str:
        """Generates the stream part of the HTTP protocol API.

        Returns:
            str: Generated streams.
        """
        # TODO - direction is 1 or 2 in HTTP!
        if self._direction is None:
            return "&".join(
                [f"stream={alias}:FIRST&stream={alias}:SECOND" for alias in self._aliases]
            )
        return "&".join([f"stream={stream}:{self._direction}" for stream in self._aliases])

    def grpc(self) -> List[MessageStream]:
        """Generates the grpc objects of the GRPC protocol API.

        Returns:
            List[MessageStream]: List of Stream with specified direction.
        """
        if self._direction is None:
            result = []
            for stream in self._aliases:
                result += [
                    MessageStream(name=stream, direction=Direction.Value("FIRST")),
                    MessageStream(name=stream, direction=Direction.Value("SECOND")),
                    # TODO - Perhaps it have to be
                    # MessageStream(name=stream, direction=Direction.FIRST),
                    # MessageStream(name=stream, direction=Direction.SECOND),
                ]
            return result
        return [
            MessageStream(name=stream, direction=Direction.Value(self._direction))
            for stream in self._aliases
        ]
