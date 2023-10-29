import enum
import http
import ipaddress
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional
from uuid import UUID

from strong_typing.auxiliary import int64
from strong_typing.inspection import extend_enum

from pysqlsync.model.key_types import PrimaryKey


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
