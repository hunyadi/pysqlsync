import enum
import http
import ipaddress
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Optional
from uuid import UUID

from strong_typing.auxiliary import (
    Annotated,
    MaxLength,
    float32,
    float64,
    int8,
    int16,
    int32,
    int64,
    uint8,
    uint16,
    uint32,
    uint64,
)
from strong_typing.inspection import extend_enum

from pysqlsync.model.key_types import PrimaryKey


class WorkflowState(enum.Enum):
    active = "active"
    inactive = "inactive"
    deleted = "deleted"


@dataclass(slots=True)
class BooleanTable:
    id: PrimaryKey[int]
    boolean: bool
    nullable_boolean: Optional[bool]


@dataclass(slots=True)
class NumericTable:
    id: PrimaryKey[int]

    signed_integer_8: int8
    signed_integer_16: int16
    signed_integer_32: int32
    signed_integer_64: int64
    unsigned_integer_8: uint8
    unsigned_integer_16: uint16
    unsigned_integer_32: uint32
    unsigned_integer_64: uint64

    integer: int
    nullable_integer: Optional[int]


@dataclass(slots=True)
class FloatTable:
    id: PrimaryKey[int]
    float_32: float32
    float_64: float64


@dataclass(slots=True)
class StringTable:
    id: PrimaryKey[int]
    arbitrary_length_string: str
    nullable_arbitrary_length_string: Optional[str]
    maximum_length_string: Annotated[str, MaxLength(255)]
    nullable_maximum_length_string: Optional[Annotated[str, MaxLength(255)]]


@dataclass(slots=True)
class DateTimeTable:
    id: PrimaryKey[int]
    iso_date_time: datetime
    iso_date: date
    iso_time: time
    optional_date_time: Optional[datetime]


@dataclass(slots=True)
class EnumTable:
    id: PrimaryKey[int]
    state: WorkflowState


@dataclass(slots=True)
class IPAddressTable:
    id: PrimaryKey[int]
    ipv4: ipaddress.IPv4Address
    ipv6: ipaddress.IPv6Address


@dataclass(slots=True)
class DataTable:
    id: PrimaryKey[int]
    data: str


@dataclass(slots=True)
class UserTable:
    id: PrimaryKey[int]
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime
    workflow_state: WorkflowState
    uuid: UUID
    name: str
    short_name: str
    sortable_name: str
    homepage_url: Optional[str]


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
class Address:
    id: PrimaryKey[int]
    city: str
    state: Optional[str] = None


@dataclass
class Location:
    id: PrimaryKey[int]
    coords: Coordinates


@dataclass
class Person:
    """
    A person.

    :param address: The address of the person's permanent residence.
    """

    id: PrimaryKey[int]
    address: Address


@dataclass
class Employee:
    id: PrimaryKey[UUID]
    name: str
    reports_to: "Employee"


@dataclass
class Teacher:
    id: PrimaryKey[UUID]
    name: str
    teaches: list[Person]


@enum.unique
class HTTPVersion(enum.Enum):
    "HTTP protocol version."

    http09 = "0.9"
    http10 = "1.0"
    http11 = "1.1"
    http20 = "2.0"


@extend_enum(http.HTTPMethod)
class HTTPMethod(enum.Enum):
    """HTTP methods used in the Canvas API."""

    SEARCH = "SEARCH"
    PROPFIND = "PROPFIND"
    LOCK = "LOCK"
    REPORT = "REPORT"
    PURGE = "PURGE"
    MKCOL = "MKCOL"
    PROPPATCH = "PROPPATCH"
    CHECKOUT = "CHECKOUT"
    UNLOCK = "UNLOCK"


HTTPStatus: type[enum.Enum] = enum.Enum(  # type: ignore
    "HTTPStatus", {e.name: str(e.value) for e in http.HTTPStatus}
)


@dataclass
class EventRecord:
    schema_version = 1

    id: PrimaryKey[int]
    guid: UUID
    timestamp: datetime
    user_id: int64
    real_user_id: int64
    expires_on: date
    interaction_duration: float
    url: str
    user_agent: Optional[str]
    http_method: HTTPMethod
    http_status: HTTPStatus
    http_version: HTTPVersion
    remote_ip: ipaddress.IPv4Address
    participated: bool
