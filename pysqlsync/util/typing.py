import sys

if sys.version_info >= (3, 10):
    from typing import ParamSpec as ParamSpec  # noqa: F401
    from typing import TypeGuard as TypeGuard  # noqa: F401
else:
    from typing_extensions import ParamSpec as ParamSpec  # noqa: F401
    from typing_extensions import TypeGuard as TypeGuard  # noqa: F401

if sys.version_info >= (3, 12):
    from typing import override as override  # noqa: F401
else:
    from typing_extensions import override as override  # noqa: F401
