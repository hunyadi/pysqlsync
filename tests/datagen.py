import datetime
import enum
import ipaddress
import random
import string
import typing
from typing import TypeVar

E = TypeVar("E", bound=enum.Enum)


def time_today(time_of_day: datetime.time) -> datetime.datetime:
    return datetime.datetime.combine(
        datetime.datetime.today(), time_of_day, time_of_day.tzinfo
    )


def random_datetime(
    start: datetime.datetime, end: datetime.datetime
) -> datetime.datetime:
    """
    Returns a random datetime between two datetime objects.
    """

    delta = end - start
    seconds = delta.days * 86400 + delta.seconds
    return start + datetime.timedelta(seconds=random.randrange(seconds))


def random_date(start: datetime.date, end: datetime.date) -> datetime.date:
    """
    Returns a random date between two date objects.
    """

    delta = end - start
    days = delta.days
    return start + datetime.timedelta(days=random.randrange(days))


def random_time(start: datetime.time, end: datetime.time) -> datetime.time:
    """
    Returns a random time between two time objects.
    """

    ts_end = time_today(end)
    ts_start = time_today(start)
    delta = ts_end - ts_start
    seconds = delta.days * 86400 + delta.seconds
    ts = ts_start + datetime.timedelta(seconds=random.randrange(seconds))
    return ts.timetz()


def random_enum(enum_type: type[E]) -> E:
    """
    Chooses an enumeration value randomly out of the possible set of values.
    """

    values = [e for e in enum_type]
    return random.choice(values)


def random_ipv4address() -> ipaddress.IPv4Address:
    """
    Creates a random IPv4 address.
    """

    return typing.cast(
        ipaddress.IPv4Address,
        ipaddress.ip_address(
            f"{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
        ),
    )


def random_alphanumeric_str(min_len: int, max_len: int) -> str:
    """
    Creates a random string of alphanumeric characters.

    :param min_len: The minimum number of characters the string should comprise of.
    :param max_len: The maximum number of characters the string should comprise of.
    """

    return "".join(
        random.choices(
            string.ascii_letters + string.digits, k=random.randint(min_len, max_len)
        )
    )
