from typing import (
    Any,
    AsyncIterator,
    BinaryIO,
    Callable,
    Generic,
    Iterable,
    Optional,
    Protocol,
    TypeVar,
)

from strong_typing.inspection import DataclassInstance, dataclass_fields
from tsv.helper import AutoDetectParser, Generator

from ..formation.inspection import reference_to_key
from ..model.properties import get_field_properties

D = TypeVar("D", bound=DataclassInstance)


def _get_extractor_fn(field_name: str) -> Callable[[Any], Any]:
    """
    Gets the value of a data-class instance attribute.

    Must be a separate function to seal context and avoid re-assignment of `field_name`.
    """

    return lambda entity: getattr(entity, field_name)


class TextWriter(Generic[D]):
    "Writes data to tab-separated values file."

    generator: Generator
    stream: BinaryIO

    _extractors: list[Callable[[Any], Any]]

    def __init__(
        self,
        stream: BinaryIO,
        entity_type: type[D],
        field_mapping: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Initializes a TSV writer.

        :param stream: The file-like object to write to.
        :param entity_type: The data-class type whose fields to persist.
        :param field_mapping: A mapping from data-class field names to TSV field names. Defines field order.
        """

        self.stream = stream
        self.generator = Generator()
        self._extractors = []

        if field_mapping is None:
            field_mapping = {
                field.name: field.name for field in dataclass_fields(entity_type)
            }

        for name in field_mapping.keys():
            self._extractors.append(_get_extractor_fn(name))

        stream.write(
            b"\t".join(name.encode("utf-8") for name in field_mapping.values())
        )
        stream.write(b"\n")

    def write_objects(self, entities: list[D]) -> None:
        for entity in entities:
            self.stream.write(
                self.generator.generate_line(
                    tuple(extractor(entity) for extractor in self._extractors)
                )
            )
            self.stream.write(b"\n")


def fields_to_types(
    entity_type: type[DataclassInstance],
    field_mapping: Optional[dict[str, str]] = None,
) -> dict[str, type]:
    """
    Creates a mapping for a TSV reader.

    :param entity_type: The data-class type whose fields to populate.
    :param field_mapping: A mapping from data-class field names to TSV field names.
    """

    if field_mapping is None:
        field_mapping = {
            field.name: field.name for field in dataclass_fields(entity_type)
        }

    return {
        field_mapping[field.name]: get_field_properties(
            reference_to_key(field.type, entity_type)
        ).tsv_type
        for field in dataclass_fields(entity_type)
    }


class TextReader:
    "Reads data from tab-separated values file."

    stream: BinaryIO
    parser: AutoDetectParser
    field_types: tuple[type, ...]

    def __init__(self, stream: BinaryIO, names_to_types: dict[str, type]) -> None:
        """
        Initializes a TSV reader.

        :param stream: The file-like object to read from.
        :param names_to_types: A mapping from field names to TSV field types.
        """

        self.stream = stream
        self.names_to_types = names_to_types

        header = stream.readline().rstrip(b"\n")

        self.parser = AutoDetectParser(names_to_types, header)
        self.field_types = tuple(names_to_types[name] for name in self.parser.columns)

    @property
    def columns(self) -> tuple[str, ...]:
        return self.parser.columns

    def read_records(self) -> list[tuple]:
        "Reads all records from the file-like object."

        rows: list[tuple] = []
        while True:
            line = self.stream.readline().rstrip(b"\n")
            if not line:
                break
            rows.append(self.parser.parse_line(line))
        return rows

    def records(self) -> Iterable[tuple]:
        "An iterator to records in a file-like object."

        while True:
            line = self.stream.readline().rstrip(b"\n")
            if not line:
                break
            yield self.parser.parse_line(line)


class AsyncBinaryIO(Protocol):
    async def readline(self, limit: int = -1) -> bytes: ...


class AsyncTextReader:
    "Reads data from tab-separated values file."

    stream: AsyncBinaryIO
    parser: AutoDetectParser
    names_to_types: dict[str, type]
    field_types: tuple[type, ...]

    def __init__(self, stream: AsyncBinaryIO, names_to_types: dict[str, type]) -> None:
        """
        Initializes a TSV reader.

        :param stream: The asynchronous file-like object to read from.
        :param names_to_types: A mapping from field names to TSV field types.
        """

        self.stream = stream
        self.names_to_types = names_to_types

    async def read_header(self) -> None:
        line = await self.stream.readline()
        header = line.rstrip(b"\n")
        self.parser = AutoDetectParser(self.names_to_types, header)
        self.field_types = tuple(
            self.names_to_types[name] for name in self.parser.columns
        )

    @property
    def columns(self) -> tuple[str, ...]:
        return self.parser.columns

    async def read_records(self) -> list[tuple]:
        "Reads all records from the file-like object."

        rows: list[tuple] = []
        while True:
            line = await self.stream.readline()
            record = line.rstrip(b"\n")
            if not record:
                break
            rows.append(self.parser.parse_line(record))
        return rows

    async def records(self) -> AsyncIterator[tuple]:
        "An asynchronous iterator to records in a file-like object."

        while True:
            line = await self.stream.readline()
            record = line.rstrip(b"\n")
            if not record:
                break
            yield self.parser.parse_line(record)
