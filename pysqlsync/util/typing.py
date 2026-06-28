import sys

if sys.version_info >= (3, 12):
    from typing import override as override  # noqa: F401
else:
    from typing_extensions import override as override  # noqa: F401
