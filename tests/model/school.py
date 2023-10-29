from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from pysqlsync.model.key_types import PrimaryKey


@dataclass
class Address:
    id: PrimaryKey[int]
    city: str
    state: Optional[str] = None


@dataclass
class Person:
    """
    A person.

    :param name: The person's full name.
    :param address: The address of the person's permanent residence.
    """

    id: PrimaryKey[int]
    name: str
    address: Address


@dataclass
class Teacher:
    id: PrimaryKey[UUID]
    name: str
    teaches: list[Person]
