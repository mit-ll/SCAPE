import re

from nose.tools import *

from scape.registry.field import Field
from scape.registry.tagged_dim import tagged_dim
from scape.registry.condition import (
    GenericBinaryCondition, GenericSetCondition, Equals, TrueCondition, And, Or, 
)
from scape.registry.table_metadata import TableMetadata
from scape.registry.data_source import DataSource

from weblog_data_source import weblog_data, get_weblog_ds

ds = get_weblog_ds()

# Data Source ##########################################################

def test_ds_default_name():
    assert_equal( ds.name , "Unknown")

@raises(NotImplementedError)
def test_not_implmented_run():
    DataSource(TableMetadata({}),description="", op_dict={}).select().run()

def test_ds_properties():
    ds.name
    ds.metadata

def test_ds_repr():
    repr(ds)

def test_ds_html():
    ds._repr_html_()

def test_ds_fields():
    assert_equal(sorted(['clientip','serverip','url','status_code','time']), ds.all_field_names)

def test_ds_get_field_names():
    assert_equal(sorted(['clientip','serverip']), sorted(ds.get_field_names(':ip')))
    assert_equal(sorted(['clientip']), ds.get_field_names('client:'))

# rewrite_generic_binary_condition

def test_ds_remove_generic_binary_condition():
    c = GenericBinaryCondition(Field('f'),'==', '23')
    r = ds._rewrite_generic_binary_condition(c)
    assert_equal( r , Equals(Field('f'), '23'))

def test_ds_unnecessary_remove_generic_binary_condition():
    c = Equals(Field('f'),'23')
    r = ds._rewrite_generic_binary_condition(c)
    assert_equal( c , r)

@raises(ValueError)
def test_ds_remove_generic_binary_condition_unsupported_op():
    c = GenericBinaryCondition(Field('f'),'<>', '23')
    ds._rewrite_generic_binary_condition(c)

# rewrite_tagged_dim
def gbceq(l,r):
    return GenericBinaryCondition(l, '==', r)

def test_ds_rewrite_tagged_dim_to_or():
    c = gbceq(tagged_dim(':ip'), '1.2.3.4')
    r = ds._rewrite_tagged_dim(c)
    expected = Or([gbceq(Field('clientip'), '1.2.3.4'), gbceq(Field('serverip'), '1.2.3.4')])
    assert_equal( expected , r)

def test_ds_rewrite_tagged_dim_nogbc():
    c = Equals(tagged_dim(':ip'), '1.2.3.4')
    r = ds._rewrite_tagged_dim(c)
    assert_equal( c , r)

@raises(ValueError)
def test_ds_rewrite_tagged_dim_no_fields():
    c = gbceq(tagged_dim(':mac'), '1:2:3:4:5:6')
    # No fields have dimension=mac
    ds._rewrite_tagged_dim(c)

# rewrite_outer_and

def test_and0():
    actual = ds._rewrite_outer_and(And([]))
    assert_equal( TrueCondition() , actual)

def test_and1():
    a = Equals(Field('a'), '1')
    actual = ds._rewrite_outer_and(And([a]))
    assert_equal( a , actual)

def test_and_rparen():
    a = Equals(Field('a'), '1')
    b = Equals(Field('b'), '1')
    c = Equals(Field('d'), '1')
    actual = ds._rewrite_outer_and(And([And([a,b]),c]))
    assert_equal( And([a,b,c]) , actual)

def test_select_all():
    assert_equal( len(ds.select().run()) , len(weblog_data))


# rewrite_generic_set_condition

def test_rewrite_set():
#    c = GenericSetCondition(Field('a'), '==', [])
#    res = ds._rewrite_generic_set_condition(c)
    assert_equal(TrueCondition(),
                 ds._rewrite_generic_set_condition(GenericSetCondition(Field('a'), '==', [])))

    assert_equal(GenericBinaryCondition(Field('a'),'==',"foo"),
                 ds._rewrite_generic_set_condition(GenericSetCondition(Field('a'), '==', ["foo"])))

    assert_equal(Or([GenericBinaryCondition(Field('a'),'==',"foo"),
                     GenericBinaryCondition(Field('a'),'==',"bar")]),
                 ds._rewrite_generic_set_condition(GenericSetCondition(Field('a'), '==', ["foo","bar"])))


# check fields

@raises(ValueError)
def test_check_fields():
    ds._check_fields(Equals(Field('a'), '1'))

def test_tags():
    assert_equal(set(['client','server','http']), ds.tags)

def test_dims():
    assert_equal(set(['ip','url','status_code','sec']), ds.dims)

def test_has_fields():
    assert_equal(ds.get_field_names('ip'), ['clientip','serverip'])
    assert_equal(ds.get_field_names('@serverip','http:url'), ['serverip','url'])
