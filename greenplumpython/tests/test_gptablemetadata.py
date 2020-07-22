import pytest
from greenplumpython.core.gptable_metadata import GPTableMetadata
def test_normal_name():
    try:
        meta = GPTableMetadata('testname', list(), 'RANDOMLY')
    except ValueError:
        pytest.fail("Unexpected ValueError ..")
    assert meta.name == 'testname'

def test_unnormal_name():
    invalid_names = ['"ab"', '"b.c"', 'public.ab.cd', 'test?name', 'test.', 'abc?']
    for name in invalid_names:
        with pytest.raises(ValueError):
            meta = GPTableMetadata(name, list(), 'RANDOMLY')
            assert meta.name == ''

def test_none_signature():
    with pytest.raises(ValueError):
        meta = GPTableMetadata('testname', None, 'RANDOMLY')

def test_wrong_type_signature():
    with pytest.raises(ValueError):
        meta = GPTableMetadata('testname', 1, 'RANDOMLY')

def test_distributedon():
    meta = GPTableMetadata('testname', list(), ['abc', 'def'])
    assert meta.distribute_on_str == 'DISTRIBUTED BY (abc, def)'
    meta = GPTableMetadata('testname', list(), ['abc', 'def'], True)
    assert meta.distribute_on_str == 'DISTRIBUTED BY ("abc", "def")'
    meta = GPTableMetadata('testname', list(), None)
    assert meta.distribute_on_str == ''
    meta = GPTableMetadata('testname', list(), 'RANDOMLY')
    assert meta.distribute_on_str == 'DISTRIBUTED RANDOMLY'
    meta = GPTableMetadata('testname', list(), 'randomly')
    assert meta.distribute_on_str == 'DISTRIBUTED RANDOMLY'
    meta = GPTableMetadata('testname', list(), 'RANDomly')
    assert meta.distribute_on_str == 'DISTRIBUTED RANDOMLY'
    meta = GPTableMetadata('testname', list(), 'replicated')
    assert meta.distribute_on_str == 'DISTRIBUTED REPLICATED'
    meta = GPTableMetadata('testname', list(), 'rePLICated')
    assert meta.distribute_on_str == 'DISTRIBUTED REPLICATED'


def test_wrong_str_distributedon():
    with pytest.raises(ValueError):
        meta = GPTableMetadata('testname', list(), 'RANDOMLYY')
        assert meta.distribute_on_str == ''

def test_wrong_type_distributedon():
    with pytest.raises(ValueError):
        meta = GPTableMetadata('testname', list(), 1)
        assert meta.distribute_on_str == ''
