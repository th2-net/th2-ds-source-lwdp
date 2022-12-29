from datetime import datetime, timezone
from th2_data_services.interfaces.utils.converter import ITimestampConverter, TimestampType


class DatetimeConverter(ITimestampConverter):
    @classmethod
    def parse_timestamp(cls, timestamp: TimestampType) -> (str, str):
        if isinstance(timestamp, datetime):
            timestamp = timestamp.replace(tzinfo=timezone.utc).timestamp()

        seconds = int(timestamp)
        nanoseconds = int((timestamp - seconds) * 1_000_000_000)
        return tuple(map(str, (seconds, nanoseconds)))

    @classmethod
    def to_milliseconds(cls, timestamp: TimestampType):
        """Converts timestamp to microseconds.

        If your timestamp has nanoseconds, they will be just cut (not rounding).

        Args:
            timestamp: TimestampType object to convert.

        Returns:
            int: Timestamp in microseconds format.
        """
        if isinstance(timestamp, tuple):
            seconds, nanoseconds = timestamp
        else:
            seconds, nanoseconds = cls.parse_timestamp(timestamp)

        if len(nanoseconds) < 9:
            return int(seconds) * 1000

        return int(f"{seconds}{nanoseconds[:-6]}")


def _check_list_or_tuple(variable, var_name):  # noqa
    if not (isinstance(variable, tuple) or isinstance(variable, list)):
        raise TypeError(f"{var_name} argument has to be list or tuple type. Got {type(variable)}")


def _check_milliseconds(dt: datetime):
    if not isinstance(dt, datetime):
        raise TypeError("Provided timestamp should be `datetime` object")
    if dt.microsecond != 0:
        raise Exception("Provided datetime shouldn't contain microseconds")
