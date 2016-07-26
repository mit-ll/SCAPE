import re
from nose.tools import *

from scape.registry.field import Field, field

# Field ################################################################

def test_field_repr():
    r = Field('f')
    assert_equal( r , eval(repr(r)))
