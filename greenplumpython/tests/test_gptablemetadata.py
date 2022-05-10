import pytest
from greenplumpython.core.gptable_metadata import GPTableMetadata


def test_normal_name():
    try:
        meta = GPTableMetadata("testname", [{"a": "int4"}], "RANDOMLY")
    except ValueError:
        pytest.fail("Unexpected ValueError ..")
    assert meta.name == "testname"


def test_unnormal_name():
    invalid_names = ['"ab"', '"b.c"', "public.ab.cd", "test?name", "test.", "abc?"]
    for name in invalid_names:
        with pytest.raises(ValueError):
            meta = GPTableMetadata(name, [{"a": "int4"}], "RANDOMLY")
            assert meta.name == ""


def test_none_signature():
    with pytest.raises(ValueError):
        meta = GPTableMetadata("testname", None, "RANDOMLY")


def test_wrong_type_signature():
    with pytest.raises(ValueError):
        meta = GPTableMetadata("testname", 1, "RANDOMLY")


def test_distributedon():
    meta = GPTableMetadata("testname", [{"a": "int4"}], ["abc", "def"])
    assert meta.distribute_on_str == "DISTRIBUTED BY (abc, def)"
    meta = GPTableMetadata("testname", [{"a": "int4"}], ["abc", "def"], True)
    assert meta.distribute_on_str == 'DISTRIBUTED BY ("abc", "def")'
    meta = GPTableMetadata("testname", [{"a": "int4"}], None)
    assert meta.distribute_on_str == ""
    meta = GPTableMetadata("testname", [{"a": "int4"}], "RANDOMLY")
    assert meta.distribute_on_str == "DISTRIBUTED RANDOMLY"
    meta = GPTableMetadata("testname", [{"a": "int4"}], "randomly")
    assert meta.distribute_on_str == "DISTRIBUTED RANDOMLY"
    meta = GPTableMetadata("testname", [{"a": "int4"}], "RANDomly")
    assert meta.distribute_on_str == "DISTRIBUTED RANDOMLY"
    meta = GPTableMetadata("testname", [{"a": "int4"}], "replicated")
    assert meta.distribute_on_str == "DISTRIBUTED REPLICATED"
    meta = GPTableMetadata("testname", [{"a": "int4"}], "rePLICated")
    assert meta.distribute_on_str == "DISTRIBUTED REPLICATED"


def test_wrong_str_distributedon():
    with pytest.raises(ValueError):
        meta = GPTableMetadata("testname", [{"a": "int4"}], "RANDOMLYY")
        assert meta.distribute_on_str == ""


def test_wrong_type_distributedon():
    with pytest.raises(ValueError):
        meta = GPTableMetadata("testname", [{"a": "int4"}], 1)
        assert meta.distribute_on_str == ""
