import unittest
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO

from pysqlsync.data.exchange import TextReader, TextWriter, fields_to_types


@dataclass
class SampleData:
    id: int
    code: str
    timestamp: datetime


class TestExchange(unittest.TestCase):
    def test_read_write(self) -> None:
        entities = [
            SampleData(1, "alpha", datetime(1989, 10, 23, 1, 0, 0)),
            SampleData(2, "beta", datetime(1989, 10, 23, 2, 0, 0)),
            SampleData(3, "gamma", datetime(1989, 10, 23, 3, 0, 0)),
        ]

        with BytesIO() as stream:
            writer = TextWriter(stream, SampleData)
            writer.write_objects(entities)
            data = stream.getvalue()

        with BytesIO(data) as stream:
            reader = TextReader(stream, fields_to_types(SampleData))
            columns, field_types = reader.columns, reader.field_types

            self.assertEqual(columns, ("id", "code", "timestamp"))
            self.assertEqual(field_types, (int, str, datetime))

            rows = reader.read_records()

            self.assertSequenceEqual(
                rows,
                [
                    (1, "alpha", datetime(1989, 10, 23, 1, 0, 0)),
                    (2, "beta", datetime(1989, 10, 23, 2, 0, 0)),
                    (3, "gamma", datetime(1989, 10, 23, 3, 0, 0)),
                ],
            )

    def test_reorder(self) -> None:
        entities = [
            SampleData(1, "alpha", datetime(1989, 10, 23, 1, 0, 0)),
            SampleData(2, "beta", datetime(1989, 10, 23, 2, 0, 0)),
            SampleData(3, "gamma", datetime(1989, 10, 23, 3, 0, 0)),
        ]
        field_mapping = {
            "timestamp": "meta.timestamp",
            "id": "key.id",
            "code": "value.code",
        }

        with BytesIO() as stream:
            writer = TextWriter(stream, SampleData, field_mapping)
            writer.write_objects(entities)
            data = stream.getvalue()

        with BytesIO(data) as stream:
            reader = TextReader(stream, fields_to_types(SampleData, field_mapping))
            columns, field_types = reader.columns, reader.field_types

            self.assertEqual(columns, ("meta.timestamp", "key.id", "value.code"))
            self.assertEqual(field_types, (datetime, int, str))

            rows = reader.read_records()

            self.assertSequenceEqual(
                rows,
                [
                    (datetime(1989, 10, 23, 1, 0, 0), 1, "alpha"),
                    (datetime(1989, 10, 23, 2, 0, 0), 2, "beta"),
                    (datetime(1989, 10, 23, 3, 0, 0), 3, "gamma"),
                ],
            )


if __name__ == "__main__":
    unittest.main()
