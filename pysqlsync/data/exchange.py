import dataclasses
from typing import BinaryIO, Generic, TypeVar

from strong_typing.inspection import DataclassInstance, dataclass_fields
from tsv.helper import AutoDetectParser, Generator

from ..formation.inspection import reference_to_key
from ..model.properties import get_field_properties

D = TypeVar("D", bound=DataclassInstance)


class TextWriter(Generic[D]):
    "Writes data to tab-separated values file."

    generator: Generator
    stream: BinaryIO

    def __init__(self, stream: BinaryIO, entity_type: type[D]) -> None:
        self.stream = stream
        self.generator = Generator()

        stream.write(
            b"\t".join(
                field.name.encode("utf-8") for field in dataclass_fields(entity_type)
            )
        )
        stream.write(b"\n")

    def write_objects(self, entities: list[D]) -> None:
        for entity in entities:
            self.stream.write(self.generator.generate_line(dataclasses.astuple(entity)))
            self.stream.write(b"\n")


class TextReader(Generic[D]):
    "Reads data from tab-separated values file."

    stream: BinaryIO
    parser: AutoDetectParser
    field_types: tuple[type, ...]

    def __init__(self, stream: BinaryIO, entity_type: type[D]) -> None:
        self.stream = stream

        names_to_types = {
            field.name: get_field_properties(
                reference_to_key(field.type, entity_type)
            ).tsv_type
            for field in dataclass_fields(entity_type)
        }
        header = stream.readline().rstrip(b"\n")

        self.parser = AutoDetectParser(names_to_types, header)
        self.field_types = tuple(names_to_types[name] for name in self.parser.columns)

    @property
    def columns(self) -> tuple[str, ...]:
        return self.parser.columns

    def read_records(self) -> list[tuple]:
        rows: list[tuple] = []
        while True:
            line = self.stream.readline().rstrip(b"\n")
            if not line:
                break
            rows.append(self.parser.parse_line(line))
        return rows
