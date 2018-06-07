import pandas as pd
from scape.pandas import datasource
from scape.registry.parsing import parse_binary_condition as C
from nose.tools import *

data = pd.DataFrame.from_records([
    {'name': 'Leona', 'age': 15, 'height': 53},
    {'name': 'Sasha', 'age': 42, 'height': 63},
    {'name': 'Chris', 'age': 34, 'height': 64},
    {'name': 'Mel', 'age':24, 'height':70},
])

meta = { 'name' : { 'tags': ['first', 'firstname'], 'dim':'name' },
         'age' : { 'tags': ['age'], 'dim':'year'},
         'height' : { 'tags': ['height'], 'dim':'inch' }}

ds = datasource(data, meta)

def test_pandas_eq():
    res = ds.select().where('firstname:=="Leona"').run()
    assert_equal (1, res.shape[0])

def test_pandas_ne():
    res = ds.select().where('firstname: != "Leona"').run()
    assert_equal (3, res.shape[0])

def test_pandas_match():
    res = ds.select().where('firstname: =~ ".*e.*"').run()
    assert_equal (2, res.shape[0])

def test_pandas_ge():
    res = ds.select().where('height:inch >= 64').run()
    assert_equal (2, res.shape[0])

def test_pandas_gt():
    res = ds.select().where('height:inch > 69').run()
    assert_equal (1, res.shape[0])

def test_pandas_lt():
    res = ds.select().where('age: < 24').run()
    assert_equal (1, res.shape[0])

def test_pandas_le():
    res = ds.select().where('age: <= 24').run()
    assert_equal (2, res.shape[0])

def test_pandas_and():
    res = ds.select().where('age: <= 24').where('height: > 60').run()
    assert_equal (1, res.shape[0])

def test_pandas_or():
    res = ds.select().where(C('age: <= 24') | C('@name == "Sasha"')).run()
    assert_equal (3, res.shape[0])

def test_pandas_trivial():
    res = ds.select().run()
    assert_equal(4, res.shape[0])
