import pytest
from greenplumpython.dataframe.metadata import Metadata
def test_normal_name():
    try:
        meta = Metadata('testname', dict(), 'RANDOMLY')
    except ValueError:
        pytest.fail("Unexpected ValueError ..")
    assert meta.name == 'testname'

def test_unnormal_name():
    with pytest.raises(ValueError):
        meta = Metadata('test?name', dict(), 'RANDOMLY')
        assert meta.name == ''

def test_none_signature():
    with pytest.raises(ValueError):
        meta = Metadata('testname', None, 'RANDOMLY')

def test_wrong_type_signature():
    with pytest.raises(ValueError):
        meta = Metadata('testname', 1, 'RANDOMLY')

def test_list_distributedon():
    meta = Metadata('testname', dict(), ['abc', 'def'])
    assert meta.distribute_on_str == 'DISTRIBUTED BY (abc, def)'

def test_none_distributedon():
    meta = Metadata('testname', dict(), None)
    assert meta.distribute_on_str == ''

def test_random_distributedon():
    meta = Metadata('testname', dict(), 'RANDOMLY')
    assert meta.distribute_on_str == 'DISTRIBUTED RANDOMLY'

def test_replicated_distributedon():
    meta = Metadata('testname', dict(), 'replicated')
    assert meta.distribute_on_str == 'DISTRIBUTED REPLICATED'

def test_wrong_str_distributedon():
    with pytest.raises(ValueError):
        meta = Metadata('testname', dict(), 'RANDOMLYY')
        assert meta.distribute_on_str == ''

def test_wrong_type_distributedon():
    with pytest.raises(ValueError):
        meta = Metadata('testname', dict(), 1)
        assert meta.distribute_on_str == ''