from dataclasses import dataclass
from uuid import uuid4

import pandas as pd

import greenplumpython as gp
import greenplumpython.experimental.file
from tests import db


def test_small_csv(db: gp.Database):
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

    res = list(db.create_dataframe(files=[csv_path], parser=parse_csv))
    assert len(res) == NUM_ROWS
    assert list(next(iter(res))) == ["i", "t"]
