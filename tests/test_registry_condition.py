import re

from nose.tools import *

from scape.registry.field import Field
from scape.registry.tag import Tag, tag
from scape.registry.dim import Dim, dim
from scape.registry.tagged_dim import TaggedDim, tagged_dim
from scape.registry.condition import (
    Condition, BinaryCondition, GenericBinaryCondition,
    Equals, MatchesCond,
    GreaterThan, GreaterThanEqualTo, 
    And, Or, or_condition,
)


# Conditions ###########################################################

def test_condition():
    c = Condition()
    assert_equal( c.fields , [])
    assert_equal( c.map(lambda x:x) , c)

### Binary Condition
def test_bc_repr():
    c = BinaryCondition(tagged_dim('t:d'), 2)
    assert_equal( c , eval(repr(c)))

### Generic Binary Condition

def test_gbc_repr():
    c = GenericBinaryCondition(tagged_dim('t:d'), '==', 2)
    assert_equal( c , eval(repr(c)))

### Equals

def test_equals_repr():
    c = Equals(tagged_dim('t:d'), 2)
    assert_equal( c , eval(repr(c)))

### Matches

def test_matches_repr():
    c = MatchesCond(tagged_dim('t:d'), 2)
    assert_equal( c , eval(repr(c)))

### Greater Than

def test_gt_repr():
    c = GreaterThan(tagged_dim('t:d'), 2)
    assert_equal( c , eval(repr(c)))

### Greater Than Equal To

def test_gteq_repr():
    c = GreaterThanEqualTo(tagged_dim('t:d'), 2)
    assert_equal( c , eval(repr(c)))

### Or

def test_or_repr():
    c = Or([Equals(tagged_dim('src:ip'), 2)])
    assert_equal( c , eval(repr(c)))

def test_or_fields():
    c = Or([Equals(Field('x'), 2)])
    assert_equal( [Field('x')] , list(c.fields))

def flip(x):
    if isinstance(x, Equals):
        return Equals(x.lhs, 2 * x.rhs)
    elif isinstance(x, Or):
        return And(x.parts)
    elif isinstance(x, And):
        return Or(x.parts)
    else:
        return x

def test_or_map():
    c = Or([Equals(Field('x'), 2)])
    actual = c.map(flip)
    expected = And([Equals(Field('x'), 4)])
    assert_equal( actual , expected)

def test_or_leaves_map():
    c = Or([Equals(Field('x'), 2)])
    actual = c.map_leaves(flip)
    expected = Or([Equals(Field('x'), 4)])
    assert_equal( actual , expected)

@raises(ValueError)
def test_or_condition0():
    or_condition([])

def test_or_condition1():
    e = Equals(Field('x'), 2)
    assert_equal( e , or_condition([e]))

def test_or_condition_many():
    cs = [Equals(Field('x'), 2), Equals('y', 3)]
    assert_equal( cs , or_condition([cs]))

### And

def test_and_repr():
    a = And([Equals(Field('x'), 2)])
    assert_equal( a , eval(repr(a)))

def test_and_fields():
    a = And([Equals(Field('x'), 2)])
    assert_equal( [Field('x')] , list(a.fields))

def test_and_map():
    x = And([Equals(Field('x'), 2)])
    actual = x.map(flip)
    expected = Or([Equals(Field('x'), 4)])
    assert_equal( actual , expected)

def test_and_map_leaves():
    a = And([Equals(Field('x'), 2)])
    assert_equal( And([Equals(Field('x'), 4)]) , a.map_leaves(flip))

