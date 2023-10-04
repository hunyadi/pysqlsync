# pysqlsync: Synchronize large volumes of data over SQL

*pysqlsync* helps you synchronize your target database or data warehouse with a data source, making efficient use of Python language elements and database drivers (client libraries). The library package employs Python data-classes (decorated with `@dataclass`) to define database tables and generate `CREATE`, `DROP`, `INSERT`, `MERGE` and `DELETE` SQL statements. Commands against the database driver are executed with the asynchronous paradigm (`async` and `await`). This can result in an order of magnitude speed increase over traditional methods such as SQLAlchemy when data is inserted or upserted (merged) into, or deleted from a table.

## Use cases

* Formation. Create an initializer SQL script (e.g. ANSI, PostgreSQL or MySQL) from a set of Python `@dataclass` definitions.
* Discovery. Create a traversable Python object hierarchy (e.g. `Namespace`, `Table`, `Column` and `Constraint` objects) from a database catalog (using `information_schema`).
* Schema synchronization. Emit SQL statements to mutate the database schema from a source state to a desired target state using `CREATE`, `DROP` and `ALTER`.
* Data import. Efficiently insert data from a list of objects or tuples of simple types into a database table using `INSERT`, `MERGE` and `DELETE` with multiple arguments and collections of objects as input.

## Quick start

First, define the table structure with a standard Python data-class (including dependent data types):

```python
from datetime import datetime
from uuid import UUID

class WorkflowState(enum.Enum):
    active = "active"
    inactive = "inactive"
    deleted = "deleted"
    
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
    homepage_url: str | None
```

The data-class can be defined statically in code, or generated dynamically from input (with `dataclasses.make_dataclass`). Fields can be required or nullable (represented in Python as `None`). All basic data types are supported, including integers (of various widths), floating-point numbers, strings (of fixed or variable length), timestamps (`datetime.datetime` in Python), UUIDs (`uuid.UUID` in Python), enumerations (represented in Python as `enum.Enum`), etc.

Next, instantiate a database engine, open a connection, create the database structure (with a `CREATE TABLE` statement), and populate the database with initial data (with SQL `INSERT` or `COPY`):

```python
engine = get_dialect("postgresql")
options = GeneratorOptions(
    enum_mode=EnumMode.RELATION, namespaces={ ... }
)
parameters = ConnectionParameters(
    host="localhost",
    port=5432,
    username="postgres",
    password=None,
    database="public",
)

data = [ UserTable(...), ... ]
async with engine.create_connection(parameters, options) as conn:
    await conn.create_objects([UserTable])
    await conn.insert_data(UserTable, data)
```

Finally, keep the target database content synchronized with data from the source (with the equivalent of SQL `MERGE`):

```python
data = [ UserTable(...), ... ]
async with engine.create_connection(parameters, options) as conn:
    await conn.upsert_data(UserTable, data)
```

## Database standard and engine support

* ANSI (when calling `str()` on Python objects such as `Table`, `Column` or `Constraint`)
* PostgreSQL (via `asyncpg`)
* MySQL (via `aiomysql`)
