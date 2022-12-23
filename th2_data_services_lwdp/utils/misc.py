from datetime import datetime, timezone


def _check_list_or_tuple(variable, var_name):  # noqa
    if not (isinstance(variable, tuple) or isinstance(variable, list)):
        raise TypeError(f"{var_name} argument has to be list or tuple type. Got {type(variable)}")


def _check_microseconds(dt: datetime):
    if dt.microsecond != 0:
        raise Exception("Provided datetime Shouldn't Contain Microseconds")


# TODO - move to DS core
def _datetime2ms(dt_timestamp: datetime):
    """Epoch time in milliseconds."""
    return int(1000 * dt_timestamp.replace(tzinfo=timezone.utc).timestamp())


# TODO - move to DS core
def _seconds2ms(timestamp: int):
    """Epoch time in milliseconds."""
    return int(1000 * timestamp)
