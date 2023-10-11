# pysqlsync: Synchronize schema and large volumes of data over SQL

*pysqlsync* helps you synchronize your target database or data warehouse with a data source, making efficient use of Python language elements and database drivers (client libraries). The library package employs Python data-classes (decorated with `@dataclass`) to define database tables and generate `CREATE`, `DROP`, `INSERT`, `MERGE` and `DELETE` SQL statements. Commands against the database driver are executed with the asynchronous paradigm (`async` and `await`). This can result in an order of magnitude speed increase over traditional methods such as SQLAlchemy when data is inserted or upserted (merged) into, or deleted from a table.

## Use cases

* Formation. Create an initializer SQL script (e.g. ANSI, PostgreSQL or MySQL) from a set of Python `@dataclass` definitions.
* Discovery. Create a traversable Python object hierarchy (e.g. `Namespace`, `Table`, `Column` and `Constraint` objects) from a database catalog (using `information_schema` or `pg_catalog`).
* Schema synchronization. Emit SQL statements to mutate the database schema from a source state to a desired target state using `CREATE`, `DROP` and `ALTER`.
* Data import. Efficiently insert data from a list of objects or tuples of simple types into a database table using `INSERT`, `MERGE` and `DELETE` with multiple arguments, and lists of tuples or collections of objects as input.

## Quick start

First, define the table structure with a standard Python data-class (including dependent data types):

```python
from datetime import datetime
from uuid import UUID
from pysqlsync.model.key_types import PrimaryKey
from strong_typing.auxiliary import Annotated, MaxLength

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
    short_name: Annotated[str, MaxLength(255)]
    homepage_url: str | None
```

The data-class can be defined statically in code, or generated dynamically from input (with `dataclasses.make_dataclass`). Fields can be required or nullable (represented in Python as `None`). All basic data types are supported, including integers (of various widths), floating-point numbers, strings (of fixed or variable length), timestamps (`datetime.datetime` in Python), UUIDs (`uuid.UUID` in Python), enumerations (represented in Python as `enum.Enum`), etc. `list[...]` is supported as a collection type, and composite types (data-classes without a primary key) are also permitted.

Next, instantiate a database engine, open a connection, create the database structure (with a `CREATE TABLE` statement), and populate the database with initial data (with SQL `INSERT` or `COPY`):

```python
from pysqlsync.factory import get_dialect
from pysqlsync.base import ConnectionParameters, GeneratorOptions

engine = get_dialect("postgresql")
parameters = ConnectionParameters(
    host="localhost",
    port=5432,
    username="postgres",
    password=None,
    database="public",
)
options = GeneratorOptions(
    enum_mode=EnumMode.RELATION, namespaces={ ... }
)

data = [ UserTable(...), ... ]
async with engine.create_connection(parameters, options) as conn:
    await conn.create_objects([UserTable])
    await conn.insert_data(UserTable, data)
```

Let's assume the database structure changes. With the help of an `Explorer` instance, discover the objects in the database, and create/drop objects to match the state as captured in the specified Python module:

```python
async with engine.create_connection(parameters, options) as conn:
    explorer = engine.create_explorer(conn)
    await explorer.synchronize(module=canvas)
```

Finally, keep the target database content synchronized with data from the source (with the equivalent of SQL `MERGE`):

```python
data = [ UserTable(...), ... ]
async with engine.create_connection(parameters, options) as conn:
    await conn.upsert_data(UserTable, data)
```

In order to boost efficiency, you can insert (or update) data directly from a list of tuples:

```python
field_names = ["id", "uuid", "name", "updated_at", ...]
field_types = [int, UUID, str, datetime, ...]
async with engine.create_connection(parameters, options) as conn:
    table = conn.get_table(UserTable)
    await conn.upsert_rows(
        table,
        field_names=field_names,
        field_types=field_types,
        records=rows,
    )
```

## Structure and data synchronization

*pysqlsync* supports two modes of operation: structure synchronization and data synchronization.

When performing *structure synchronization*, *pysqlsync* morphs a database source state into a desired target state. The source state is typically obtained with reflection (e.g. extracting metadata from `information_schema` or `pg_catalog`). The target state is defined as a set of Python data-classes either statically or dynamically (e.g. based on a [JSON Schema](https://json-schema.org/)). Comparing source and target state, *pysqlsync* creates a transformation script (predominantly ANSI SQL with vendor-specific extensions such as comments), and runs the script against an asynchronous client such as `asyncpg` or `aiomysql`. This script creates new schemas, structure and enumeration types, tables, columns, constraints, etc. whenever the target state contains items that the source state lacks, and drops database objects when the opposite is true. When there are matching objects (based on qualified name), the object is mutated (e.g. the data type of a column is changed).

Once database structure has been morphed into the desired state, *data synchronization* helps keep a local database state in sync with a remote database state. *pysqlsync* implements insert and upsert functions to handle lists of tuples of data. Each tuple corresponds to a table row, and each tuple member is a column entry. Data in each column may have to be transformed to be suitable for the database dialect, e.g. MySQL would have to transform a `UUID` into a `BINARY(16)`, represented in Python as `bytes` because it has no `uuid` type. These transformation functions are derived in advance such that there is minimum CPU load to process a record (one of possibly billions). Loading data from network/disk to memory may use an efficient parser implementation such as [tsv2py](https://github.com/hunyadi/tsv2py), which significantly speeds up parse time for composite types such as `datetime` or `UUID` with the help of SIMD CPU instructions.

## Database standard and engine support

* ANSI (when calling `str()` on Python objects such as `Table`, `Column` or `Constraint`)
* PostgreSQL (via `asyncpg`)
* MySQL (via `aiomysql`)

## Python classes for database objects

*pysqlsync* features several Python classes that correspond to database objects:

* `Catalog` captures a database state of possibly several namespaces.
* `Namespace` corresponds to a database schema in engines that feature schemas (e.g. PostgreSQL).
* `Table` represents a database table with a primary key, several columns and constraints.
* `Column` objects declare data type and nullability, and a possibly hold a default value.
* `StructType` and `StructMember` represent composite types in engines that support them (e.g. PostgreSQL).
* `EnumType` captures enumeration types for engines that represent them as their own type (e.g. PostgreSQL).

The collection of database objects represents a current structure, which may morph into a target structure. Some of the objects may be vendor-specific, e.g. MySQL has its own `MySQLColumn` type and PostgreSQL has its own `PostgreSQLTable` type.

All objects are identified either with a local ID (e.g. namespaces and columns) or a qualified ID (e.g. tables). When the database engine lacks namespace support, qualified IDs map to a prefix, e.g. `canvas.accounts` (table `accounts` in namespace `canvas`) becomes `canvas__accounts` (a table with no namespace).
