from datetime import datetime


def _check_list_or_tuple(variable, var_name):  # noqa
    if not (isinstance(variable, tuple) or isinstance(variable, list)):
        raise TypeError(f"{var_name} argument has to be list or tuple type. Got {type(variable)}")


def _check_datetime(dt: datetime):
    if not isinstance(dt, datetime):
        raise TypeError("Provided timestamp should be `datetime` object")
