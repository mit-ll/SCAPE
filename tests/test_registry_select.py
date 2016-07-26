import re

from nose.tools import *

from scape.registry.field import Field
from scape.registry.condition import (
    And, Equals,
)

from weblog_data_source import get_weblog_ds

ds = get_weblog_ds()

# Select ###############################################################

def test_select_repr():
    # TODO DataSource not round tripping
    repr(ds.select(':ip'))

def test_select_copy():
    q = ds.select(':ip')
    q2 = q.copy()
    # TODO __eq__ for select, need to account for attrs
#    assert q == q2

def test_select_inital_condition():
    assert_equal(ds.select().condition, And([]))

def test_select_ds_args():
    ds.select(begin='now').ds_args

def test_add_where():
    q = ds.select(':ip')
    q2 = q.where(':ip==1.2.3.4')
    q3 = q.where(Equals(Field('clientip'), '1.2.3.4'))

@raises(ValueError)
def test_add_where_badarg():
    q = ds.select(':ip')
    q2 = q.where(1)

def test_select_set_fields():
    q = ds.select(':ip')
    q2 = q.with_fields(':mac')

def test_select_check():
    ds.select(':ip').check()

def test_select_debug():
    ds.select(':ip').debug()

def test_select_fields():
    pass
#    print(ds.select(':ip').run())
#    assert [Field('clientip'), Field('serverip')] == ds.select(':ip')._fields
