from dataclasses import dataclass
from uuid import uuid4

import pandas as pd

import greenplumpython as gp
import greenplumpython.experimental.file
from tests import db


def test_csv_single_chunk(db: gp.Database):
    NUM_ROWS = 10
    df = pd.DataFrame({"i": range(NUM_ROWS), "t": ["a" * 10 for _ in range(NUM_ROWS)]})
    csv_path = f"/tmp/test_{uuid4().hex}.csv"
    df.to_csv(csv_path, index=False)

    @dataclass
    class IntAndText:
        i: int
        t: str

    @gp.create_function
    def parse_csv(path: str) -> list[IntAndText]:
        import csv

        with open(path) as csv_file:
            for row in csv.DictReader(csv_file):
                yield row

    df_from_gp = pd.DataFrame.from_records(
        iter(db.create_dataframe(files=[csv_path], parser=parse_csv))
    )
    assert df.equals(df_from_gp)


def test_csv_multi_chunks(db: gp.Database):
    # Set the chunk size to be 3 bytes (< size of int in C)
    # so that data is guaranteed to be splitted into multiple chunks.
    default_chunk_size = greenplumpython.experimental.file._CHUNK_SIZE
    greenplumpython.experimental.file._CHUNK_SIZE = 3
    assert greenplumpython.experimental.file._CHUNK_SIZE == 3
    test_csv_single_chunk(db)
    greenplumpython.experimental.file._CHUNK_SIZE = default_chunk_size
