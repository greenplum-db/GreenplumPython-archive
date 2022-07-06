from typing import List

import pytest

import greenplumpython as gp


@pytest.fixture
def db():
    db = gp.database(host="localhost", dbname="gpadmin")
    yield db
    db.close()


def test_op_on_consts(db: gp.Database):
    regex_match = gp.binaryOperator("~", db)
    # FIXME: Remove the single quotes after implementing the const wrapper
    result = list(regex_match("'hello'", "h.*o").rename("is_matched").to_table().fetch())
    assert len(result) == 1 and result[0]["is_matched"]


def test_op_index(db: gp.Database):
    import json

    class Student:
        def __init__(self, name: str, grade: int, courses: List[str]) -> None:
            self.name = name
            self.grade = grade
            self.courses = courses

    john = Student("john", 9, ["math", "english"])
    # FIXME: Remove type casting after implementing the const wrapper
    rows = [(f"'{json.dumps(john.__dict__)}'::jsonb",)]
    student = gp.values(rows, db=db, column_names=["info"]).save_as("student", temp=True)
    db.execute("CREATE INDEX student_name ON student USING gin (info)", has_results=False)

    db.set_config("enable_seqscan", False)
    json_contains = gp.binaryOperator("@>", db)
    results = student[json_contains(student["info"], json.dumps({"name": "john"}))].explain()
    uses_index_scan = False
    for row in results:
        if "Index Scan" in row["QUERY PLAN"]:
            uses_index_scan = True
            break
    assert uses_index_scan


# FIXME : Add test for unary operator
