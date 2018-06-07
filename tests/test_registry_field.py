import re
from nose.tools import *

from scape.registry.field import Field, field

# Field ################################################################

def test_field_repr():
    r = Field('f')
    assert_equal( r , eval(repr(r)))

@raises(ValueError)
def test_field_empty():
    field('')

def test_field_strips_at():
    assert_equal(field('@f'), Field('f'))
