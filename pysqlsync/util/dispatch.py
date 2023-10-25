import asyncio
import concurrent.futures
import functools
from typing import Callable, Coroutine, ParamSpec, TypeVar

R = TypeVar("R")
P = ParamSpec("P")


def thread_dispatch(fn: Callable[P, R]) -> Callable[P, Coroutine[None, None, R]]:
    "A decorator to transform a synchronous function into an asynchronous function dispatched to a thread pool."

    @functools.wraps(fn)
    async def invoke(*args: P.args, **kwargs: P.kwargs) -> R:
        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return await loop.run_in_executor(pool, fn, *args, **kwargs)

    return invoke
