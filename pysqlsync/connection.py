"""
pysqlsync: Synchronize schema and large volumes of data.

This module helps create a secure sockets layer (SSL) context.

:see: https://github.com/hunyadi/pysqlsync
"""

import enum
import ssl
import sys
from typing import Optional

if sys.version_info >= (3, 10):
    import truststore
else:
    import certifi


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


def create_context(ssl_mode: ConnectionSSLMode) -> Optional[ssl.SSLContext]:
    if ssl_mode is None or ssl_mode is ConnectionSSLMode.disable:
        return None
    elif (
        ssl_mode is ConnectionSSLMode.prefer
        or ssl_mode is ConnectionSSLMode.allow
        or ssl_mode is ConnectionSSLMode.require
    ):
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    elif ssl_mode is ConnectionSSLMode.verify_ca:
        if sys.version_info >= (3, 10):
            ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        else:
            ctx = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH, cafile=certifi.where()
            )
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED
        return ctx
    elif ssl_mode is ConnectionSSLMode.verify_full:
        if sys.version_info >= (3, 10):
            ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        else:
            ctx = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH, cafile=certifi.where()
            )
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        return ctx
    else:
        raise ValueError(f"unsupported SSL mode: {ssl_mode}")
