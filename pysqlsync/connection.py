"""
pysqlsync: Synchronize schema and large volumes of data.

This module helps create a secure sockets layer (SSL) context.

Copyright 2023-2026, Levente Hunyadi

:see: https://github.com/hunyadi/pysqlsync
"""

import enum
import ssl
from dataclasses import dataclass
from urllib.parse import quote

import truststore


@enum.unique
class ConnectionSSLMode(enum.Enum):
    # SSL is disabled.
    disable = "disable"

    # Try SSL first, fallback to non-SSL connection if SSL connection fails.
    prefer = "prefer"

    # Try without SSL first, then retry with SSL if the first attempt fails.
    allow = "allow"

    # Force an SSL connection. Certificate verification errors are ignored.
    require = "require"

    # Force an SSL connection, and verify that the server certificate is issued by a trusted
    # certificate authority (CA).
    verify_ca = "verify-ca"

    # Force an SSL connection, verify that the server certificate is issued by a trusted CA,
    # and that the requested server host name matches that in the certificate.
    verify_full = "verify-full"


def create_context(ssl_mode: ConnectionSSLMode) -> ssl.SSLContext | None:
    "Creates an SSL context to pass to a database connection object."

    if ssl_mode is ConnectionSSLMode.disable:
        return None
    elif ssl_mode is ConnectionSSLMode.prefer or ssl_mode is ConnectionSSLMode.allow or ssl_mode is ConnectionSSLMode.require:
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    elif ssl_mode is ConnectionSSLMode.verify_ca:
        ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED
        return ctx
    elif ssl_mode is ConnectionSSLMode.verify_full:
        ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        return ctx
    else:
        raise ValueError(f"unsupported SSL mode: {ssl_mode}")


@dataclass
class ConnectionParameters:
    """
    Database connection parameters that would typically be encapsulated in a connection string.

    :param host: Database server to connect to.
    :param port: Port to use for the connection, `None` for default.
    :param username: User identifier to log in with.
    :param password: Password to log in with.
    :param database: Database name to select as default (a.k.a. `USE`).
    :param ssl: Connection mode for trusted connections.
    """

    host: str | None = None
    port: int | None = None
    username: str | None = None
    password: str | None = None
    database: str | None = None
    ssl: ConnectionSSLMode | None = None

    def __str__(self) -> str:
        host = self.host or "localhost"
        port = f":{self.port}" if self.port else ""
        username = f"{quote(self.username, safe='')}@" if self.username else ""
        database = f"/{quote(self.database, safe='')}" if self.database else ""
        ssl = f"?ssl={self.ssl.value}" if self.ssl else ""
        return f"{username}{host}{port}{database}{ssl}"
