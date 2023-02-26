import enum
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

from pysqlsync.properties import PrimaryKey


class WorkflowState(enum.Enum):
    active = "active"
    inactive = "inactive"
    deleted = "deleted"


@dataclass(slots=True)
class NumericTable:
    id: PrimaryKey[int]

    boolean: bool
    nullable_boolean: Optional[bool]

    signed_integer_8: int8
    signed_integer_16: int16
    signed_integer_32: int32
    signed_integer_64: int64
    unsigned_integer_8: uint8
    unsigned_integer_16: uint16
    unsigned_integer_32: uint32
    unsigned_integer_64: uint64

    float_32: float32
    float_64: float64

    integer: int
    nullable_integer: Optional[int]


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
