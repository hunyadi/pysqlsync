from collections.abc import ItemsView, KeysView, Mapping, ValuesView
from typing import Generic, Iterable, Iterator, Optional, TypeVar, Union, overload

from ..model.id_types import SupportsName

T = TypeVar("T")
ObjectItem = TypeVar("ObjectItem", bound=SupportsName)


class ObjectDict(Generic[ObjectItem], Mapping[str, ObjectItem]):
    _items: dict[str, ObjectItem]

    def __init__(self, items: Iterable[ObjectItem]) -> None:
        self._items = {item.name.local_id: item for item in items}

    def __contains__(self, key: object) -> bool:
        return key in self._items

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, ObjectDict):
            return False

        return self._items == value._items

    def __getitem__(self, key: str) -> ObjectItem:
        return self._items[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __repr__(self) -> str:
        return repr(list(self._items.values()))

    @overload
    def get(self, key: str, /) -> Optional[ObjectItem]: ...

    @overload
    def get(self, key: str, /, default: Union[ObjectItem, T]) -> Union[ObjectItem, T]: ...

    def get(self, key: str, /, default: Optional[T] = None) -> Union[None, ObjectItem, T]:
        return self._items.get(key, default)

    def add(self, item: ObjectItem) -> None:
        if item.name.local_id in self._items:
            raise ValueError(f"item already in collection: {item.name}")

        self._items[item.name.local_id] = item

    def remove(self, key: str) -> None:
        self._items.pop(key)

    def keys(self) -> KeysView[str]:
        return self._items.keys()

    def values(self) -> ValuesView[ObjectItem]:
        return self._items.values()

    def items(self) -> ItemsView[str, ObjectItem]:
        return self._items.items()

    def difference(self, op: "ObjectDict[ObjectItem]") -> Iterable["ObjectItem"]:
        for key, item in self.items():
            if key not in op:
                yield item

    def intersection(self, op: "ObjectDict[ObjectItem]") -> Iterable[tuple["ObjectItem", "ObjectItem"]]:
        for key, item in self.items():
            op_item = op.get(key)
            if op_item is not None:
                yield (item, op_item)

    def sort(self) -> None:
        self._items = dict(sorted(self._items.items()))

    def reorder(self, order: list[str]) -> None:
        self._items = {key: self._items[key] for key in order}
