from typing import AsyncIterable, TypeVar

T = TypeVar("T")


async def unsync(items: AsyncIterable[T]) -> list[T]:
    result: list[T] = []
    async for item in items:
        result.append(item)
    return result
