from typing import List

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

    db._execute(
        f"""
        DROP TYPE IF EXISTS test.complex_number CASCADE;
        CREATE TYPE test.complex_number AS (r float8, i float8);
        """,
        has_results=False,
    )
    db._execute(
        f"CREATE FUNCTION test.complex_add(a test.complex_number, b test.complex_number)"
        f"RETURNS test.complex_number\n"
        f"AS $$\n"
        f"return {'{'}\n"
        f"    'r': a['r'] + b['r'],\n"
        f"    'i': a['i'] + b['i']\n"
        f"{'}'}\n"
        f"$$"
        f"LANGUAGE plpython3u;\n"
        f"DROP OPERATOR IF EXISTS test.+(complex_number, complex_number);\n"
        f"CREATE OPERATOR test.+ ("
        f"leftarg = test.complex_number,"
        f"rightarg = test.complex_number,"
        f"function = test.complex_add,"
        f"commutator = +"
        f");",
        has_results=False,
    )
    complex = gp.type_("complex_number", schema="test")
    complex_op = gp.operator("+", db=db, schema="test")
    result = db.assign(complex_add=lambda: (complex_op(complex("(1, 2)"), complex("(1, 2)"))))
    for row in result:
        assert row["complex_add"] == {"i": 4, "r": 2}
    complex_add = gp.function("complex_add", schema="test")
    result = db.apply(lambda: complex_add(complex("(1, 2)"), complex("(1, 2)")), expand=True)
    for row in result:
        assert row["r"] == 2 and row["i"] == 4


# FIXME : Add test for unary operator
