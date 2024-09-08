import abc
import logging
import os
import os.path

from pysqlsync.base import BaseEngine
from pysqlsync.connection import ConnectionParameters, ConnectionSSLMode
from pysqlsync.factory import get_dialect


class TestEngineBase(abc.ABC):
    @property
    @abc.abstractmethod
    def engine(self) -> BaseEngine: ...

    @property
    @abc.abstractmethod
    def parameters(self) -> ConnectionParameters: ...


class OracleBase(TestEngineBase):
    "Base class for testing Oracle features."

    @property
    def engine(self) -> BaseEngine:
        return get_dialect("oracle")

    @property
    def parameters(self) -> ConnectionParameters:
        return ConnectionParameters(
            host="localhost",
            port=1521,
            username="system",
            password="<?YourStrong@Passw0rd>",
            database="FREEPDB1",
        )


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
            password="<?YourStrong@Passw0rd>",
            database="levente.hunyadi",
            ssl=ConnectionSSLMode.prefer,
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
            password="<?YourStrong@Passw0rd>",
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
            password="<?YourStrong@Passw0rd>",
            database="levente_hunyadi",
            ssl=ConnectionSSLMode.require,
        )


class RedshiftBase(TestEngineBase):
    "Base class for testing AWS Redshift features."

    @property
    def engine(self) -> BaseEngine:
        return get_dialect("redshift")

    @property
    def parameters(self) -> ConnectionParameters:
        return ConnectionParameters(
            host=os.getenv("REDSHIFT_HOST"),
            port=5439,
            username="admin",
            password="<YourStrong.Passw0rd>",
            database="dev",
        )


def has_env_var(name: str) -> bool:
    """
    True if tests are to be executed. To be used with `@unittest.skipUnless`.

    :param name: Environment variable to check.
    """

    return os.environ.get(f"TEST_{name}", "0") == "1"


def configure() -> None:
    """
    Configures logging in unit and integration tests. To be invoked in module `__main__`.
    """

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    ch = logging.FileHandler(os.path.join(os.path.dirname(__file__), "test.log"), "w")
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

    os.environ["TEST_INTEGRATION"] = "1"
    os.environ["TEST_POSTGRESQL"] = "1"
    os.environ["TEST_MYSQL"] = "1"
    # os.environ["TEST_ORACLE"] = "1"
    # os.environ["TEST_MSSQL"] = "1"
    # os.environ["TEST_REDSHIFT"] = "1"
