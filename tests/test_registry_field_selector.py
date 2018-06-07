import re
from nose.tools import *

from scape.registry.field import Field
from scape.registry.tagged_dim import tagged_dim, TaggedDim
from scape.registry.utils import field_or_tagged_dim

def test_field_selector():
    assert_equal( field_or_tagged_dim(' @f '), Field('f'))
    assert_equal(field_or_tagged_dim(Field('f')), Field('f'))

    assert_equal( field_or_tagged_dim('tag:dim'), tagged_dim('tag:dim'))
    assert_equal( field_or_tagged_dim(['tag','dim']), tagged_dim('tag:dim'))

    assert_equal( field_or_tagged_dim(tagged_dim('tag:dim')), tagged_dim('tag:dim'))

@raises(ValueError)
def test_empty_field_selector():
    field_or_tagged_dim('')
