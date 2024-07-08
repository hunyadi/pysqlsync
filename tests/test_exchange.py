import types
import unittest
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Optional

from pysqlsync.data.exchange import (
    AsyncTextReader,
    TextReader,
    TextWriter,
    fields_to_types,
)


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
        content = [
            (1, "alpha", datetime(1989, 10, 23, 1, 0, 0)),
            (2, "beta", datetime(1989, 10, 23, 2, 0, 0)),
            (3, "gamma", datetime(1989, 10, 23, 3, 0, 0)),
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
            self.assertSequenceEqual(rows, content)

        with BytesIO(data) as stream:
            reader = TextReader(stream, fields_to_types(SampleData))

            rows = []
            for record in reader.records():
                rows.append(record)
            self.assertSequenceEqual(rows, content)

    def test_reorder(self) -> None:
        entities = [
            SampleData(1, "alpha", datetime(1989, 10, 23, 1, 0, 0)),
            SampleData(2, "beta", datetime(1989, 10, 23, 2, 0, 0)),
            SampleData(3, "gamma", datetime(1989, 10, 23, 3, 0, 0)),
        ]
        content = [
            (datetime(1989, 10, 23, 1, 0, 0), 1, "alpha"),
            (datetime(1989, 10, 23, 2, 0, 0), 2, "beta"),
            (datetime(1989, 10, 23, 3, 0, 0), 3, "gamma"),
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
            self.assertSequenceEqual(rows, content)


class AsyncBytesIO:
    buf: BytesIO

    def __init__(self, data: bytes):
        self.buf = BytesIO(data)

    async def readline(self, limit: int = -1) -> bytes:
        return self.buf.readline(limit)

    async def __aenter__(self) -> "AsyncBytesIO":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[types.TracebackType],
    ) -> None:
        pass


class TestAsyncExchange(unittest.IsolatedAsyncioTestCase):
    async def test_read_write(self) -> None:
        entities = [
            SampleData(1, "alpha", datetime(1989, 10, 23, 1, 0, 0)),
            SampleData(2, "beta", datetime(1989, 10, 23, 2, 0, 0)),
            SampleData(3, "gamma", datetime(1989, 10, 23, 3, 0, 0)),
        ]
        content = [
            (1, "alpha", datetime(1989, 10, 23, 1, 0, 0)),
            (2, "beta", datetime(1989, 10, 23, 2, 0, 0)),
            (3, "gamma", datetime(1989, 10, 23, 3, 0, 0)),
        ]

        with BytesIO() as stream:
            writer = TextWriter(stream, SampleData)
            writer.write_objects(entities)
            data = stream.getvalue()

        async with AsyncBytesIO(data) as stream:
            reader = AsyncTextReader(stream, fields_to_types(SampleData))
            await reader.read_header()
            columns, field_types = reader.columns, reader.field_types

            self.assertEqual(columns, ("id", "code", "timestamp"))
            self.assertEqual(field_types, (int, str, datetime))

            rows = await reader.read_records()
            self.assertSequenceEqual(rows, content)

        async with AsyncBytesIO(data) as stream:
            reader = AsyncTextReader(stream, fields_to_types(SampleData))
            await reader.read_header()

            rows = []
            async for record in reader.records():
                rows.append(record)
            self.assertSequenceEqual(rows, content)


if __name__ == "__main__":
    unittest.main()
