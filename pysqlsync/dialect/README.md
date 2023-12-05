# Dialects

pysqlsync comes with several database dialects shipped with the library. However, it is possible to create and register new dialects that behave the same way as built-in dialects. In terms of capabilities, there are no differences between built-in and user-defined dialects.

If you are about to write integration for a new database dialect, it is recommended that you take one of the existing dialects (e.g. PostgreSQL, Microsoft SQL Server or MySQL), and use it as a template.

## Register a new dialect

New dialects are added by calling the function `pysqlsync.factory.register_dialect`. The dialect has to derive from `BaseEngine`, which is a factory class for generator, connection and explorer objects. Each dialect has to provide its own implementation of `BaseEngine`, which returns the appropriate class types.

## Generator

The generator class is the most fundamental of the three base classes, and is responsible for generating SQL statements for creating or dropping database objects (e.g. tables, structs or enums), and inserting, updating or deleting data. The base class for a generator is `BaseGenerator`.

A generator maintains a state, either created explicitly in code or discovered from a database. A state is a collection of Python objects that represent the set of namespaces (schemas), tables, structs, enums, constraints, etc. that exist in a database.

A generator encapsulates a data-class converter (an instance of `DataclassConverter`), which takes a Python data-class type and returns a collection of Python objects that may in turn be used to generate SQL. The data-class converter is identical across dialects but each dialect passes different configuration to the converter to account for differences between dialects, e.g. PostgreSQL has a dedicated `enum` type, MySQL and Oracle have in-line enumeration types, and Microsoft SQL Server has no enumeration type at all. By passing the appropriate configuration options to the data-class converter, the dialect implementation can ensure that the appropriate Python objects are instantiated either for database objects or database data types. For example, the PostgreSQL dialect would use `PostgreSQLJsonType` instead of the basic `SqlJsonType` to represent the PostgreSQL-specific compact JSON type [jsonb](https://www.postgresql.org/docs/current/datatype-json.html).

A generator also holds a reference to an object factory. The object factory ensures that the data-class converter or the explorer instantiate the dialect-specific Python object types. For example, the PostgreSQL dialect has `PostgreSQLTable`, which differs from the ANSI-compliant `Table` in that it can associate comments with table objects.

Finally, the generator has a mutator, whose job is to emit SQL statements to transform a database source state into a target state. In general, the mutator is the same across dialects, inheritance helps specialize the implementation to account for dialect-specific differences. For example, PostgreSQL or MySQL have comments for database objects, and the mutator can help add/remove comments when a database object is modified.

In writing a specialization for a generator, one would pass the appropriate configuration options to the data-class converter in the `__init__` method, override methods such as `get_table_insert_stmt` to tell what SQL statement is generated when data is about to be inserted/updated/deleted, and override methods such as `get_value_transformer` to define mappings between native Python types and the binding types the database driver (e.g. `asyncpg`) is expecting.

## Connection and context

The connection class represents an active connection to the database server. It holds a native connection object (e.g. `asyncpg.Connection`). When the connection is opened, it yields a context object. The dialect-specific context class may possibly override when happens on operations such as executing a SQL statement, running a query, or inserting data.

pysqlsync runs in an asynchronous context. Many database drivers (e.g. `asyncpg` or `aiomysql`) support asynchronous operations natively. For database drivers that require sycnhronous calls, it is the responsibility of the connection implementation to dispatch the task to a worker thread. pysqlsync provides the decorator `thread_dispatch` for this purpose.

## Explorer

The role of the explorer class is to use `information_schema` (or its dialect-specific equivalent such as `pg_catalog` in PostgreSQL or `sys` in Microsoft SQL server) to create a hierarchy of Python objects that represent the database state. By comparing the source state with a target state, the generator can emit SQL statements that mutate the database, e.g. create/drop database objects, or modify existing objects.

If the database dialect supports `information_schema`, the explorer implementation can simply inherit from `AnsiExplorer`, and override only those methods where the database dialect has additional features (e.g. identity columns in Microsoft SQL Server, or `auto_increment` or column comments in MySQL).
