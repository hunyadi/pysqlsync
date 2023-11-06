import sys

if sys.version_info >= (3, 10):
    from typing import ParamSpec as ParamSpec
    from typing import TypeGuard as TypeGuard
else:
    from typing_extensions import ParamSpec as ParamSpec
    from typing_extensions import TypeGuard as TypeGuard

if sys.version_info >= (3, 12):
    from typing import override as override
else:
    from typing_extensions import override as override
