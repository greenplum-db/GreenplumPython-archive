from dataclasses import dataclass
import greenplumpython as gp

from tests import db


def test_simple_func(db: gp.Database):
    @dataclass
    class Int:
        i: int

    @gp.create_function(language_handler="plcontainer", runtime="plc_python_example")
    def add_one(x: list[Int]) -> list[Int]:
        return [{"i": arg["i"] + 1} for arg in x]

    assert (
        len(
            list(
                db.create_dataframe(columns={"i": range(10)}).apply(
                    lambda _: add_one(), expand=True
                )
            )
        )
        == 10
    )
