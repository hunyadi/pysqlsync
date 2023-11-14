import asyncio
import functools
import inspect
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Coroutine, TypeVar

from .typing import ParamSpec

R = TypeVar("R")
P = ParamSpec("P")


def thread_dispatch(fn: Callable[P, R]) -> Callable[P, Coroutine[None, None, R]]:
    "A decorator to transform a synchronous function into an asynchronous function dispatched to a thread pool."

    if not callable(fn):
        raise TypeError("expected: a callable")

    if inspect.iscoroutinefunction(fn):
        raise TypeError("expected: a regular function; got: an async function")

    @functools.wraps(fn)
    async def invoke(*args: P.args, **kwargs: P.kwargs) -> R:
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=1) as pool:
            return await loop.run_in_executor(
                pool, functools.partial(fn, *args, **kwargs)
            )

    return invoke
