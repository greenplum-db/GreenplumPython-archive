import pytest
from greenplumpython.core.gptable_metadata import GPTableMetadata
def test_normal_name():
    try:
        meta = GPTableMetadata('testname', list(), 'RANDOMLY')
    except ValueError:
        pytest.fail("Unexpected ValueError ..")
    assert meta.name == 'testname'

def test_unnormal_name():
    with pytest.raises(ValueError):
        meta = GPTableMetadata('test?name', list(), 'RANDOMLY')
        assert meta.name == ''

def test_none_signature():
    with pytest.raises(ValueError):
        meta = GPTableMetadata('testname', None, 'RANDOMLY')

def test_wrong_type_signature():
    with pytest.raises(ValueError):
        meta = GPTableMetadata('testname', 1, 'RANDOMLY')

def test_list_distributedon():
    meta = GPTableMetadata('testname', list(), ['abc', 'def'])
    assert meta.distribute_on_str == 'DISTRIBUTED BY (abc, def)'

def test_none_distributedon():
    meta = GPTableMetadata('testname', list(), None)
    assert meta.distribute_on_str == ''

def test_random_distributedon():
    meta = GPTableMetadata('testname', list(), 'RANDOMLY')
    assert meta.distribute_on_str == 'DISTRIBUTED RANDOMLY'

def test_replicated_distributedon():
    meta = GPTableMetadata('testname', list(), 'replicated')
    assert meta.distribute_on_str == 'DISTRIBUTED REPLICATED'

def test_wrong_str_distributedon():
    with pytest.raises(ValueError):
        meta = GPTableMetadata('testname', list(), 'RANDOMLYY')
        assert meta.distribute_on_str == ''

def test_wrong_type_distributedon():
    with pytest.raises(ValueError):
        meta = GPTableMetadata('testname', list(), 1)
        assert meta.distribute_on_str == ''