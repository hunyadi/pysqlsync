from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID

from pysqlsync.model.key_types import PrimaryKey


@dataclass
class UserTable:
    id: PrimaryKey[int]
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime
    uuid: UUID
    name: str
    short_name: str
    sortable_name: str
    homepage_url: Optional[str]
