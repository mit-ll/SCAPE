# Copyright (2016) Massachusetts Institute of Technology.
# Reproduction/Use of all or any part of this material shall
# acknowledge the MIT Lincoln Laboratory as the source under the
# sponsorship of the US Air Force Contract No. FA8721-05-C-0002.

# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

import re

from nose.tools import *

import pyparsing

from scape.registry.field import Field
from scape.registry.tag import Tag, tag
from scape.registry.dim import Dim, dim
from scape.registry.tagged_dim import TaggedDim, tagged_dim
from scape.registry.condition import GenericBinaryCondition, GenericSetCondition
from scape.registry.parsing import (
    parse_binary_condition, parse_list_fieldselectors, 
    rhs_value_p, rhs_set_p, rhs_p
)

def test_parse_rhs():
    assert_equal(rhs_value_p().parseString('23'), 23)
    assert_equal(rhs_value_p().parseString('"FOO"'), "FOO")
    assert_equal(rhs_value_p().parseString("'FOO'"), "FOO")
    assert_equal(rhs_value_p().parseString('2.3'), 2.3)
    assert_equal(rhs_value_p().parseString('1.2.3.4'), '1.2.3.4')

def test_parse_rhs_set():
    assert_equal([1,2,3], rhs_set_p().parseString('{1,2,3}')[:])

def test_parse_rhs():
    assert_equal(99, rhs_p().parseString('99')['value'])
    assert_equal("FOO", rhs_p().parseString('"FOO"')['value'])
    assert_equal([91,92,93], rhs_p().parseString('{91,92,93}')['valueset'][:][:])


# Binary Condition #####################################################

def test_parse_field_eq_num():
    assert_equal( parse_binary_condition('@asdf == 23') ,
                  GenericBinaryCondition(Field('asdf'), '==', 23))
def test_parse_field_eq_float():
    assert_equal( parse_binary_condition('@asdf == 2.03'),
                  GenericBinaryCondition(Field('asdf'), '==', 2.03))
def test_parse_field_eq_dblquote():
    assert_equal( parse_binary_condition('@asdf == "asdf"'),
                  GenericBinaryCondition(Field('asdf'), '==', "asdf"))
def test_parse_field_eq_quote():
    assert_equal( parse_binary_condition("@asdf == 'asdf'"),
                  GenericBinaryCondition(Field('asdf'), '==', "asdf"))
def test_parse_field_eq_star_end():
    assert_equal( parse_binary_condition('@asdf == "test*"'),
                  GenericBinaryCondition(Field('asdf'), '==', "test*"))
def test_parse_field_eq_star():
    assert_equal( parse_binary_condition('@asdf == "*test*"'),
                  GenericBinaryCondition(Field('asdf'), '==', "*test*"))
def test_parse_field_eq_ip():
    assert_equal( parse_binary_condition('@asdf == 2.3.4.5'),
                  GenericBinaryCondition(Field('asdf'), '==', "2.3.4.5"))

def test_parse_rhs_valueset():
    assert_equal(GenericSetCondition(Field('asdf'), '==', [91,92,93]),
                 parse_binary_condition('@asdf == {91,92,93}'))

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
    assert_equal( parse_binary_condition(':dim == 23'),
                  GenericBinaryCondition(TaggedDim(frozenset(), Dim('dim')), '==', 23))

def test_parse_tag_eq_num():
    parse_binary_condition('tag: == 23')
def test_parse_tagdim_eq_num():
    assert_equal( parse_binary_condition('tag:dim == 23'),
                  GenericBinaryCondition(TaggedDim(frozenset([Tag('tag')]), Dim('dim')), '==', 23))
def test_parse_tags_eq_num():
    parse_binary_condition("tag1:tag2 == 23")


def test_parse_set():
    parse_binary_condition("@asdf == {1, 2, 3}")

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
