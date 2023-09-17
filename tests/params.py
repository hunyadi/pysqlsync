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


class MySQLBase(TestEngineBase):
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
