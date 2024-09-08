import asyncio
import dataclasses
import enum
import logging
import os.path
import sys
import uuid
from datetime import datetime
from typing import Optional

from strong_typing.auxiliary import Annotated, MaxLength

from pysqlsync.base import GeneratorOptions
from pysqlsync.connection import ConnectionParameters
from pysqlsync.factory import get_dialect
from pysqlsync.formation.py_to_sql import EnumMode
from pysqlsync.model.id_types import LocalId
from pysqlsync.model.key_types import PrimaryKey

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ch = logging.FileHandler(os.path.join(os.path.dirname(__file__), "test.log"), "w")
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


# BLOCK: Example 1
class WorkflowState(enum.Enum):
    active = "active"
    inactive = "inactive"
    deleted = "deleted"


@dataclasses.dataclass
class UserTable:
    id: PrimaryKey[int]
    updated_at: datetime
    workflow_state: WorkflowState
    uuid: uuid.UUID
    name: str
    short_name: Annotated[str, MaxLength(255)]
    homepage_url: Optional[str] = None


# END


async def run() -> None:
    tables = sys.modules[UserTable.__module__]

    # BLOCK: Example 2
    engine = get_dialect("postgresql")
    parameters = ConnectionParameters(
        host="localhost",
        port=5432,
        username="levente.hunyadi",
        password=None,
        database="levente.hunyadi",
    )
    options = GeneratorOptions(
        enum_mode=EnumMode.RELATION, namespaces={tables: "example"}
    )

    data = [
        UserTable(
            id=1,
            updated_at=datetime.now(),
            workflow_state=WorkflowState.active,
            uuid=uuid.uuid4(),
            name="Laura Twenty-Four",
            short_name="Laura",
        )
    ]
    async with engine.create_connection(parameters, options) as conn:
        await conn.create_objects([UserTable])
        await conn.insert_data(UserTable, data)
    # END

    # BLOCK: Example 3
    async with engine.create_connection(parameters, options) as conn:
        await engine.create_explorer(conn).synchronize(module=tables)
    # END

    # BLOCK: Example 4
    data = [
        UserTable(
            id=2,
            updated_at=datetime.now(),
            workflow_state=WorkflowState.active,
            uuid=uuid.uuid4(),
            name="Zeta Twelve",
            short_name="Zeta",
        )
    ]
    async with engine.create_connection(parameters, options) as conn:
        await engine.create_explorer(conn).synchronize(module=tables)
        await conn.upsert_data(UserTable, data)

    # END

    # BLOCK: Example 5
    field_names = ["id", "uuid", "name", "short_name", "workflow_state", "updated_at"]
    field_types = [int, uuid.UUID, str, str, str, datetime]
    rows = [
        (1, uuid.uuid4(), "Laura Twenty-Four", "Laura", "active", datetime.now()),
        (2, uuid.uuid4(), "Zeta Twelve", "Zeta", "inactive", datetime.now()),
    ]
    async with engine.create_connection(parameters, options) as conn:
        await engine.create_explorer(conn).synchronize(module=tables)
        table = conn.get_table(UserTable)
        await conn.upsert_rows(
            table,
            field_names=tuple(field_names),
            field_types=tuple(field_types),
            records=rows,
        )
    # END

    async with engine.create_connection(parameters, options) as conn:
        await conn.drop_schema(LocalId("example"))


asyncio.run(run())
