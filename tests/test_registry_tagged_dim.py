import re
from nose.tools import *

from scape.registry.tag import Tag, tag
from scape.registry.dim import Dim, dim
from scape.registry.tagged_dim import TaggedDim, tagged_dim

# TaggedDim ##############################################################

@raises(ValueError)
def test_tagged_dim_str():
    TaggedDim(tags=["asdf"])

@raises(ValueError)
def test_tagged_dim_baddim():
    TaggedDim(dim=1)

@raises(ValueError)
def test_tagged_dim_badarg():
    tagged_dim(1)

def test_tagged_dim_repr():
    t = tagged_dim("tag1:dim")
    assert_equal( t , eval(repr(t)))

def test_tagged_dim_to_dict():
    t = tagged_dim("tag1:dim")
    assert_equal({'tags':['tag1'], 'dim':'dim'}, t.to_dict())

def test_parse_tagged_dim():
    assert_equal( tagged_dim(['tag1', 'dim']) , TaggedDim(dim=Dim('dim'), tags=[Tag('tag1')]))
    assert_equal( tagged_dim('dim') , TaggedDim(dim=Dim('dim')))
    assert_equal( tagged_dim(':dim') , TaggedDim(dim=Dim('dim')))
    assert_equal( tagged_dim('tag1:') , TaggedDim(tags=[Tag('tag1')]))
    assert_equal( tagged_dim('tag1:dim') , TaggedDim(dim=Dim('dim'), tags=[Tag('tag1')]))
    assert_equal( tagged_dim('tag1:tag2:dim') , TaggedDim(dim=Dim('dim'), tags=[Tag('tag1'), Tag('tag2')]))

def test_parse_field():
    print(tagged_dim('@Field'))
    # assert_equal( tagged_dim('@field'), Field('f'))
