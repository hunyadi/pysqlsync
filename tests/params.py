import abc

from pysqlsync.base import BaseEngine, ConnectionParameters
from pysqlsync.factory import get_dialect


class TestEngineBase(abc.ABC):
    @abc.abstractproperty
    def engine(self) -> BaseEngine:
        ...

    @abc.abstractproperty
    def parameters(self) -> ConnectionParameters:
        ...


class PostgreSQLBase(TestEngineBase):
    "Base class for testing PostgreSQL features."

    @property
    def engine(self) -> BaseEngine:
        return get_dialect("postgresql")

    @property
    def parameters(self) -> ConnectionParameters:
        return ConnectionParameters(
            host="localhost",
            port=5432,
            username="levente.hunyadi",
            password=None,
            database="levente.hunyadi",
        )


class MSSQLBase(TestEngineBase):
    "Base class for testing Microsoft SQL Server features."

    @property
    def engine(self) -> BaseEngine:
        return get_dialect("mssql")

    @property
    def parameters(self) -> ConnectionParameters:
        return ConnectionParameters(
            host="127.0.0.1",
            port=None,
            username="SA",
            password="<YourStrong@Passw0rd>",
            database=None,
        )


class MySQLBase(TestEngineBase):
    "Base class for testing MySQL features."

    @property
    def engine(self) -> BaseEngine:
        return get_dialect("mysql")

    @property
    def parameters(self) -> ConnectionParameters:
        return ConnectionParameters(
            host="localhost",
            port=3306,
            username="root",
            password=None,
            database="levente_hunyadi",
        )
