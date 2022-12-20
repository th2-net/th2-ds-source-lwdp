from datetime import datetime, timezone


def check_list_or_tuple(variable, var_name):  # noqa
    if not (isinstance(variable, tuple) or isinstance(variable, list)):
        raise TypeError(f"{var_name} argument has to be list or tuple type. Got {type(variable)}")


# TODO - move to DS core
def datetime2ms(dt_timestamp: datetime):
    """Epoch time in milliseconds."""
    return int(1000 * dt_timestamp.replace(tzinfo=timezone.utc).timestamp())


# TODO - move to DS core
def seconds2ms(timestamp: int):
    """Epoch time in milliseconds."""
    return int(1000 * timestamp)
