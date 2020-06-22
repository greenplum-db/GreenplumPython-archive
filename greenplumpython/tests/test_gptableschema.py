import pytest
from greenplumpython.dataframe.gptable_schema import GPTableSchema
def test_normal_name():
    try:
        meta = GPTableSchema('testname', dict(), 'RANDOMLY')
    except ValueError:
        pytest.fail("Unexpected ValueError ..")
    assert meta.name == 'testname'

def test_unnormal_name():
    with pytest.raises(ValueError):
        meta = GPTableSchema('test?name', dict(), 'RANDOMLY')
        assert meta.name == ''

def test_none_signature():
    with pytest.raises(ValueError):
        meta = GPTableSchema('testname', None, 'RANDOMLY')

def test_wrong_type_signature():
    with pytest.raises(ValueError):
        meta = GPTableSchema('testname', 1, 'RANDOMLY')

def test_list_distributedon():
    meta = GPTableSchema('testname', dict(), ['abc', 'def'])
    assert meta.distribute_on_str == 'DISTRIBUTED BY (abc, def)'

def test_none_distributedon():
    meta = GPTableSchema('testname', dict(), None)
    assert meta.distribute_on_str == ''

def test_random_distributedon():
    meta = GPTableSchema('testname', dict(), 'RANDOMLY')
    assert meta.distribute_on_str == 'DISTRIBUTED RANDOMLY'

def test_replicated_distributedon():
    meta = GPTableSchema('testname', dict(), 'replicated')
    assert meta.distribute_on_str == 'DISTRIBUTED REPLICATED'

def test_wrong_str_distributedon():
    with pytest.raises(ValueError):
        meta = GPTableSchema('testname', dict(), 'RANDOMLYY')
        assert meta.distribute_on_str == ''

def test_wrong_type_distributedon():
    with pytest.raises(ValueError):
        meta = GPTableSchema('testname', dict(), 1)
        assert meta.distribute_on_str == ''