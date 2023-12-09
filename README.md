# pysqlsync: Synchronize schema and large volumes of data over SQL

*pysqlsync* helps you synchronize your target database or data warehouse with a data source, making efficient use of Python language elements and database drivers (client libraries). The library package employs Python data-classes (decorated with `@dataclass`) to define database tables and generate `CREATE`, `DROP`, `INSERT`, `MERGE` and `DELETE` SQL statements. Commands against the database driver are executed with the asynchronous paradigm (`async` and `await`). This can result in an order of magnitude speed increase over traditional methods such as SQLAlchemy when data is inserted or upserted (merged) into, or deleted from a table.

## Use cases

* Formation. Create an initializer SQL script (e.g. ANSI, PostgreSQL or MySQL) from a set of Python `@dataclass` definitions.
* Discovery. Create a traversable Python object hierarchy (e.g. `Namespace`, `Table`, `Column` and `Constraint` objects) from a database catalog (using `information_schema` or `pg_catalog`).
* Schema synchronization. Emit SQL statements to mutate the database schema from a source state to a desired target state using `CREATE`, `DROP` and `ALTER`.
* Data import. Efficiently insert data from a list of objects or tuples of simple types into a database table using `INSERT`, `MERGE` and `DELETE` with multiple arguments, and lists of tuples or collections of objects as input.

## Quick start

First, define the table structure with a standard Python data-class (including dependent data types):

<!-- Example 1 -->
```python
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
```

The data-class can be defined statically in code, or generated dynamically from input (with `dataclasses.make_dataclass`). Fields can be required or nullable (represented in Python as `None`). All basic data types are supported, including integers (of various widths), floating-point numbers, strings (of fixed or variable length), timestamps (`datetime.datetime` in Python), UUIDs (`uuid.UUID` in Python), enumerations (represented in Python as `enum.Enum`), etc. `list[...]` is supported as a collection type, and composite types (data-classes without a primary key) are also permitted.

Next, instantiate a database engine, open a connection, create the database structure (with a `CREATE TABLE` statement), and populate the database with initial data (with SQL `INSERT` or `COPY`):

<!-- Example 2 -->
```python
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
```

Let's assume the database structure changes. With the help of an `Explorer` instance, discover the objects in the database, and create/drop objects to match the state as captured in the specified Python module:

<!-- Example 3 -->
```python
async with engine.create_connection(parameters, options) as conn:
    await engine.create_explorer(conn).synchronize(module=tables)
```

Finally, keep the target database content synchronized with data from the source (with the equivalent of SQL `MERGE`):

<!-- Example 4 -->
```python
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
```

In order to boost efficiency, you can insert (or update) data directly from a list of tuples:

<!-- Example 5 -->
```python
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
```

## Structure and data synchronization

*pysqlsync* supports two modes of operation: structure synchronization and data synchronization.

When performing *structure synchronization*, *pysqlsync* morphs a database source state into a desired target state. The source state is typically obtained with reflection (e.g. extracting metadata from `information_schema` or `pg_catalog`). The target state is defined as a set of Python data-classes either statically or dynamically (e.g. based on a [JSON Schema](https://json-schema.org/)). Comparing source and target state, *pysqlsync* creates a transformation script (predominantly ANSI SQL with vendor-specific extensions such as comments), and runs the script against an asynchronous client such as `asyncpg` or `aiomysql`. This script creates new schemas, structure and enumeration types, tables, columns, constraints, etc. whenever the target state contains items that the source state lacks, and drops database objects when the opposite is true. When there are matching objects (based on qualified name), the object is mutated (e.g. the data type of a column is changed).

Once database structure has been morphed into the desired state, *data synchronization* helps keep a local database state in sync with a remote database state. *pysqlsync* implements insert and upsert functions to handle lists of tuples of data. Each tuple corresponds to a table row, and each tuple member is a column entry. Data in each column may have to be transformed to be suitable for the database dialect, e.g. MySQL would have to transform a `UUID` into a `BINARY(16)`, represented in Python as `bytes` because it has no `uuid` type. These transformation functions are derived in advance such that there is minimum CPU load to process a record (one of possibly billions). Loading data from network/disk to memory may use an efficient parser implementation such as [tsv2py](https://github.com/hunyadi/tsv2py), which significantly speeds up parse time for composite types such as `datetime` or `UUID` with the help of SIMD CPU instructions.

## Formation

Formation is the process of generating a series of SQL statements from a collection of Python data-class definitions. Several formation modes are supported.

### Enumerations

`EnumMode` determines how Python enumeration types (subclasses of `enum.Enum`) are converted into database object types. Possible options for the target of an enumeration type are:

* a SQL `ENUM` type created with `CREATE TYPE ... AS ENUM ( ... )` (PostgreSQL), or
* an inline SQL `ENUM` definition, e.g. `ENUM('a', 'b', 'c')` (MySQL and Oracle), or
* a SQL data type corresponding to the enumeration value type, and a `CHECK` constraint on the column to block invalid values, or
* a foreign/primary key relation (reference constraint), coupled with a lookup table, in which the lookup table consists of an identity column acting as the primary key and a unique column storing the enumeration values.

### Data-classes

`StructMode` determines how to convert composite types that are not mapped into SQL tables. A data-class type may be converted into

* a composite SQL type with `CREATE TYPE ... AS ( ... )` (PostgreSQL), or
* the SQL `json` type (PostgreSQL), or
* a text type, e.g. `varchar`, which holds the data as serialized JSON.

### Lists

`ArrayMode` determines how to treat sequence types, such as Python lists, i.e. whether to represent them as

* a SQL `array` type (PostgreSQL), or
* a JSON array stored in a column with the SQL data type `json` (PostgreSQL), or
* a serialized JSON string stored in a text column type, e.g. `varchar`.

### Modules

`GeneratorOptions` allows you to pass a mapping between module types and SQL schema identifiers. Objects in each Python module would be transformed into SQL objects in the corresponding SQL schema (namespace).

## Database standard and engine support

* ANSI (when calling `str()` on Python objects such as `Table`, `Column` or `Constraint`)
* PostgreSQL (via `asyncpg`)
* Microsoft SQL Server (via `pyodbc`)
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

## Dialects

pysqlsync comes with several database dialects shipped with the library. However, it is possible to create and register new dialects that behave the same way as built-in dialects. In terms of capabilities, there are no differences between built-in and user-defined dialects.

If you are about to write integration for a new database dialect, it is recommended that you take one of the existing dialects (e.g. PostgreSQL, Microsoft SQL Server or MySQL), and use it as a template. For more information, explore the folder `pysqlsync/dialect`.
