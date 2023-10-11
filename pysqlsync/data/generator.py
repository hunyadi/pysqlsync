import datetime
import decimal
import enum
import random
import string
import typing
import uuid
from ipaddress import IPv4Address, IPv6Address, ip_address
from socket import AF_INET, AF_INET6, inet_ntop
from struct import pack
from typing import Any, Callable, Optional, TypeVar

from strong_typing.auxiliary import IntegerRange, MaxLength, MinLength
from strong_typing.inspection import (
    DataclassInstance,
    dataclass_fields,
    get_annotation,
    is_type_enum,
    is_type_literal,
    is_type_union,
    unwrap_literal_values,
    unwrap_union_types,
)
from strong_typing.serialization import object_to_json

from pysqlsync.formation.inspection import is_struct_type, reference_to_key
from pysqlsync.model.properties import get_field_properties

T = TypeVar("T")
E = TypeVar("E", bound=enum.Enum)


class DataGeneratorError(RuntimeError):
    pass


def random_bool() -> bool:
    return random.uniform(0.0, 1.0) > 0.5


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


def shuffled(items: list[T]) -> list[T]:
    items = items.copy()
    random.shuffle(items)
    return items


def random_enum_generator(
    enum_type: type[E],
    *,
    count: Optional[int] = None,
    min_count: Optional[int] = None,
    max_count: Optional[int] = None,
) -> Callable[[], list[E]]:
    values = [e for e in enum_type]
    if count is not None:
        if count >= len(values):
            return lambda: shuffled(values)
        else:
            return lambda: shuffled(values)[:count]
    elif min_count is not None or max_count is not None:
        min_c = min_count or 0
        max_c = max_count or 4
        c = max_c - min_c
        return lambda: shuffled(values)[
            : min(min_c + random.randint(0, c), len(values))
        ]
    else:
        raise TypeError("invalid parameter combination")


def random_ipv4addr() -> IPv4Address:
    "Creates a random IPv4 address."

    return typing.cast(
        IPv4Address,
        ip_address(inet_ntop(AF_INET, pack(">L", random.getrandbits(32)))),
    )


def random_ipv6addr() -> IPv6Address:
    "Creates a random IPv6 address."

    return typing.cast(
        IPv6Address,
        ip_address(
            inet_ntop(
                AF_INET6, pack(">QQ", random.getrandbits(64), random.getrandbits(64))
            )
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


P = TypeVar("P")
R = TypeVar("R")


def call_repeat(generator: Callable[[P], R], arg: P, max_count: int) -> list[R]:
    count = random.randint(0, max_count)
    return [generator(arg) for _ in range(count)]


class RandomGenerator:
    """
    Generates a data-class instances with all their fields populated with random values recursively.
    """

    keys: list[int]

    def __init__(self, count: int) -> None:
        """
        Initializes the generator to a sample size.

        :param count: The total number of objects to generate.
        """

        self.keys = list(range(count))
        random.shuffle(self.keys)

    def create_json(self, cls: type) -> Callable[[int], dict[str, Any]]:
        generators = {
            field.name: self.create(field.type, cls) for field in dataclass_fields(cls)
        }
        return lambda k: {
            field_name: generator(k) for field_name, generator in generators.items()
        }

    def create(self, typ: Any, cls: type[DataclassInstance]) -> Callable[[int], Any]:
        """
        Creates a generator for a random value.

        :param typ: The type of the value to generate.
        :param cls: The context in which the type occurs, used for evaluating forward reference types.
        :returns: A callable object that takes a sequence index and returns a random value.
        """

        properties = get_field_properties(reference_to_key(typ, cls))
        plain_type = properties.plain_type
        field_type = properties.field_type

        if properties.is_primary:
            if plain_type is int:
                return lambda k: self.keys[k]
            elif plain_type is uuid.UUID:
                return lambda _: uuid.uuid4()

            raise DataGeneratorError(f"unknown key type: {plain_type}")

        if properties.nullable:
            optional_generator = self.create(field_type, cls)
            return (
                lambda k: optional_generator(k)
                if random.uniform(0.0, 1.0) > 0.1
                else None
            )
        elif plain_type is bool:
            return lambda _: random_bool()
        elif plain_type is int:
            integer_range = get_annotation(field_type, IntegerRange)
            if integer_range is not None:
                minimum_value = integer_range.minimum
                maximum_value = integer_range.maximum
                return lambda _: random.randint(minimum_value, maximum_value)

            return lambda _: random.randint(0, 1000000)
        elif plain_type is float:
            return lambda _: random.uniform(0.0, 1.0)
        elif plain_type is datetime.datetime:
            return lambda _: random_datetime(
                datetime.datetime(1982, 10, 23, 2, 30, 0, tzinfo=datetime.timezone.utc),
                datetime.datetime.now().replace(tzinfo=datetime.timezone.utc),
            )
        elif plain_type is datetime.date:
            return lambda _: random_date(
                datetime.date(1982, 10, 23),
                datetime.date.today(),
            )
        elif plain_type is str:
            min_length_tag = get_annotation(field_type, MinLength)
            min_length = min_length_tag.value if min_length_tag is not None else 0
            max_length_tag = get_annotation(field_type, MaxLength)
            max_length = max_length_tag.value if max_length_tag is not None else 255
            return lambda _: random_alphanumeric_str(min_length, max_length)
        elif plain_type is decimal.Decimal:
            return lambda _: decimal.Decimal.from_float(random.uniform(0, 1000))
        elif plain_type is uuid.UUID:
            return lambda _: uuid.uuid4()
        elif plain_type is IPv4Address:
            return lambda _: random_ipv4addr()
        elif plain_type is IPv6Address:
            return lambda _: random_ipv6addr()
        elif is_type_enum(plain_type):
            return lambda _: random_enum(plain_type)  # type: ignore
        elif is_type_literal(plain_type):
            literal_values = unwrap_literal_values(plain_type)
            if len(literal_values) > 1:
                return lambda k: random.choice(literal_values)[k]
            else:
                literal_value = literal_values[0]
                return lambda _: literal_value
        elif is_type_union(plain_type):
            union_types = unwrap_union_types(plain_type)
            union_generators = [
                self.create(union_type, cls) for union_type in union_types
            ]
            return lambda k: random.choice(union_generators)(k)
        elif is_struct_type(plain_type):
            json_generator = self.create_json(plain_type)
            return lambda k: object_to_json(json_generator(k))

        origin_type = typing.get_origin(plain_type)
        if origin_type is list:
            (element_type,) = typing.get_args(plain_type)
            if is_type_enum(element_type):
                enum_generator = random_enum_generator(element_type, max_count=5)
                return lambda _: object_to_json(enum_generator())
            else:
                element_generator = self.create(element_type, cls)
                return lambda k: object_to_json(call_repeat(element_generator, k, 5))

        raise DataGeneratorError(f"unknown value type: {plain_type}")


D = TypeVar("D", bound=DataclassInstance)


def random_objects(cls: type[D], count: int) -> list[D]:
    """
    Creates a list of data-class instances with all their fields populated with random values recursively.
    """

    random_generator = RandomGenerator(count)

    generators = {
        field.name: random_generator.create(field.type, cls)
        for field in dataclass_fields(cls)
    }

    items: list[D] = []
    for k in range(0, count):
        args = {
            field_name: generator(k) for field_name, generator in generators.items()
        }
        items.append(cls(**args))

    return items
