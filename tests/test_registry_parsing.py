import re

from nose.tools import *

import pyparsing

from scape.registry.field import Field
from scape.registry.tag import Tag, tag
from scape.registry.dim import Dim, dim
from scape.registry.tagged_dim import TaggedDim, tagged_dim
from scape.registry.condition import GenericBinaryCondition
from scape.registry.parsing import (
    parse_binary_condition, parse_list_fieldselectors, 
)

# Binary Condition #####################################################

def test_parse_field_eq_num():
    assert_equal( parse_binary_condition('@asdf == 23') , GenericBinaryCondition(Field('asdf'), '==', 23))
def test_parse_field_eq_float():
    parse_binary_condition('@asdf == 2.03')
def test_parse_field_eq_dblquote():
    parse_binary_condition('@asdf == "asdf"')
def test_parse_field_eq_quote():
    parse_binary_condition("@asdf == 'asdf'")
def test_parse_field_eq_ip():
    parse_binary_condition('@asdf == 2.3.4.5')

# We don't yet support ()'s, ands, ors
#def test_parse_field_eq_parens():
#    parse_binary_condition('(@asdf == 2.3.4.5)')

@raises(pyparsing.ParseException)
def test_parse_field_eq_ip_extra_garbage():
    parse_binary_condition('@asdf == 2.3.4.5  ffff')

@raises(pyparsing.ParseException)
def test_parse_field_eq_unquoted():
    parse_binary_condition("@asdf == asdf")

def test_parse_dim_eq_num():
    parse_binary_condition(':dim == 23')
def test_parse_tag_eq_num():
    parse_binary_condition('tag: == 23')
def test_parse_tagdim_eq_num():
    parse_binary_condition('tag:dim == 23')
def test_parse_tags_eq_num():
    parse_binary_condition("tag1:tag2 == 23")


# Field Selectors ######################################################

def test_parse_tagdim_field_list_fields1():
    def f(x):
        res = parse_list_fieldselectors(x)
        return res
    assert_equal( f("*"),[])
    assert_equal( f(""),[])
    assert_equal( f("@F"),[Field('F')])
    assert_equal( f("@F,@G"),[Field('F'),Field('G')])
    assert_equal( f(":dim"),[tagged_dim("dim")])
    assert_equal( f("tag:,:dim"),[tagged_dim('tag:'),tagged_dim("dim")])

@raises(pyparsing.ParseException)
def test_bad_fieldselectors():
    parse_list_fieldselectors("@categories == 'General'")
