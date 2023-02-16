from typing import List

import pytest

import greenplumpython as gp
from tests import db


def test_op_on_consts(db: gp.Database):
    regex_match = gp.operator("~", db)
    result = db.assign(is_matched=lambda: regex_match("hello", "h.*o"))
    assert len(list(result)) == 1 and next(iter(result))["is_matched"]


def test_op_index(db: gp.Database):
    import json

    class Student:
        def __init__(self, name: str, grade: int, courses: List[str]) -> None:
            self.name = name
            self.grade = grade
            self.courses = courses

    john = Student("john", 9, ["math", "english"])
    jsonb = gp.type_("jsonb")
    rows = [(jsonb(json.dumps(john.__dict__)),)]
    student = db.create_dataframe(rows=rows, column_names=["info"]).save_as(
        "student", temp=True, column_names=["info"]
    )
    db._execute("CREATE INDEX student_name ON student USING gin (info)", has_results=False)

    db._execute("SET enable_seqscan TO False", has_results=False)
    json_contains = gp.operator("@>", db)
    results = student[lambda t: json_contains(t["info"], json.dumps({"name": "john"}))]._explain()
    uses_index_scan = False
    for row in results:
        if "Index Scan" in row["QUERY PLAN"]:
            uses_index_scan = True
            break
    assert uses_index_scan


def test_op_func_type_with_schema(db: gp.Database):
    my_add = gp.operator("+", db=db)
    result = db.assign(add=lambda: my_add(1, 2))
    for row in result:
        assert row["add"] == 3
    wrong_add = gp.operator("pg_catalog.+", db=db)
    result = db.assign(add=lambda: wrong_add(1, 2))
    with pytest.raises(Exception) as exc_info:
        print(result)
    assert "cross-database references are not implemented: pg_catalog.pg_catalog." in str(
        exc_info.value
    )


# FIXME : Add test for unary operator
