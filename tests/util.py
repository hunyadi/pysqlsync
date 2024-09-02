from typing import Iterable, Iterator, TypeVar


class IteratorExhausted(RuntimeError):
    "Exception raised when `next(...)` is called on an exhausted iterator."


T = TypeVar("T")


class reuse_guard(Iterable[T]):
    """
    Wraps an iterable such that it can be iterated only once.

    Example:
    ```
    rows = [1, 2, 3, 4, 5, 6, 7, 8, 9]

    iterable: Iterable[int] = (row for row in rows)
    for _ in iterable:
        pass
    for _ in iterable:
        self.fail()  # never called

    iterable = reuse_guard(row for row in rows)
    for _ in iterable:
        pass
    with self.assertRaises(IteratorExhausted):
        for _ in iterable:  # raises IteratorExhausted
            pass
    ```
    """

    iterator: Iterator[T]
    exhausted: bool

    def __init__(self, iterable: Iterable[T]):
        self.iterator = iter(iterable)
        self.exhausted = False

    def __next__(self) -> T:
        if self.exhausted:
            raise IteratorExhausted("iterator already exhausted")

        try:
            return next(self.iterator)
        except StopIteration as e:
            self.exhausted = True
            raise e

    def __iter__(self) -> Iterator[T]:
        if self.exhausted:
            raise IteratorExhausted("iterator already exhausted")

        return self
