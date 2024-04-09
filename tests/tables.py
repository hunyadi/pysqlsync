import enum
import ipaddress
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Literal, Optional, Union
from uuid import UUID

from strong_typing.auxiliary import (
    Annotated,
    MaxLength,
    Precision,
    TimePrecision,
    float32,
    float64,
    int8,
    int16,
    int32,
    int64,
)

from pysqlsync.model.key_types import DEFAULT, Identity, PrimaryKey, Unique


class ExtensibleEnum(enum.Enum):
    "Extensible enumerations must have at most one member."

    unspecified = "__unspecified__"


class WorkflowState(enum.Enum):
    "Regular enumerations must have at least two members."

    active = "active"
    inactive = "inactive"
    deleted = "deleted"


@dataclass
class BooleanTable:
    id: PrimaryKey[int]
    boolean: bool
    nullable_boolean: Optional[bool]


@dataclass
class NumericTable:
    id: PrimaryKey[int]

    integer_8: int8
    integer_16: int16
    integer_32: int32
    integer_64: int64

    integer: int
    nullable_integer: Optional[int]


@dataclass
class DefaultNumericTable:
    id: PrimaryKey[int]

    integer_8: int8 = 127
    integer_16: int16 = 32767
    integer_32: int32 = 2147483647
    integer_64: int64 = 0

    integer: int = 23


@dataclass
class FixedPrecisionFloatTable:
    id: PrimaryKey[int]
    float_32: float32
    float_64: float64
    optional_float_32: Optional[float32]
    optional_float_64: Optional[float64]


@dataclass
class VariablePrecisionFloatTable:
    id: PrimaryKey[int]
    float_value: float
    # float_precision: Annotated[float, Precision(5, 2)]


@dataclass
class DecimalTable:
    id: PrimaryKey[int]
    decimal_value: Decimal
    optional_decimal: Optional[Decimal]
    decimal_precision: Annotated[Decimal, Precision(5, 2)]


@dataclass
class StringTable:
    id: PrimaryKey[int]
    arbitrary_length_string: str
    nullable_arbitrary_length_string: Optional[str]
    maximum_length_string: Annotated[str, MaxLength(128)]
    nullable_maximum_length_string: Optional[Annotated[str, MaxLength(128)]]


@dataclass
class DateTimeTable:
    id: PrimaryKey[int]
    iso_date_time: datetime
    iso_date: date
    iso_time: time
    optional_date_time: Optional[datetime]
    timestamp_precision: Annotated[datetime, TimePrecision(6)]


@dataclass
class DefaultDateTimeTable:
    id: PrimaryKey[int]
    iso_date_time: datetime = datetime(1989, 10, 24, 23, 59, 59, tzinfo=timezone.utc)


@dataclass
class EnumTable:
    id: PrimaryKey[int]
    state: WorkflowState
    optional_state: Optional[WorkflowState]


@dataclass
class EnumArrayTable:
    id: PrimaryKey[int]
    states: list[WorkflowState]


@dataclass
class EnumSetTable:
    id: PrimaryKey[int]
    states: set[WorkflowState]


@dataclass
class ExtensibleEnumTable:
    id: PrimaryKey[int]
    state: Union[ExtensibleEnum, Annotated[str, MaxLength(64)]]
    optional_state: Union[ExtensibleEnum, Annotated[str, MaxLength(64)], None]


@dataclass
class IPAddressTable:
    id: PrimaryKey[int]
    ipv4: ipaddress.IPv4Address
    ipv6: ipaddress.IPv6Address
    ipv4_or_ipv6: Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
    optional_ipv4: Optional[ipaddress.IPv4Address]
    optional_ipv6: Optional[ipaddress.IPv6Address]


@dataclass
class LiteralTable:
    id: PrimaryKey[int]
    single: Literal["value"]
    multiple: Literal["one", "of", "many"]
    union: Union[Literal["unknown"], Annotated[str, MaxLength(255)]]
    unbounded: Union[Literal["unknown"], str]


@dataclass
class DataTable:
    id: PrimaryKey[int]
    data: str


@dataclass
class UniqueTable:
    id: PrimaryKey[Identity[int]]
    unique: Unique[Annotated[str, MaxLength(64)]]


UniqueTable(id=DEFAULT, unique="unique")
UniqueTable(id=23, unique="unique")


@dataclass
class Coordinates:
    """
    Coordinates in the geographic coordinate system.

    :param lat: Latitude in degrees.
    :param long: Longitude in degrees.
    """

    lat: float
    long: float


@dataclass
class Location:
    id: PrimaryKey[int]
    coords: Coordinates


@dataclass
class Address:
    id: PrimaryKey[int]
    city: str
    state: Optional[str] = None


@dataclass
class Person:
    """
    A person.

    :param name: The person's full name.
    :param address: The address of the person's permanent residence.
    """

    id: PrimaryKey[int]
    name: str
    address: Address


@dataclass
class Employee:
    id: PrimaryKey[UUID]
    name: str
    reports_to: "Employee"
