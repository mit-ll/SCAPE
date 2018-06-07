import re
from nose.tools import *

from scape.registry.tag import Tag, tag

# Tag ##################################################################

def test_tag_repr():
    r = Tag('tag')
    assert_equal( r , eval(repr(r)))

def test_tag_html():
    Tag('g')._repr_html_()

@raises(ValueError)
def test_bad_arg():
    Tag(1)

def test_tag():
    assert_equal( tag('t') , Tag('t'))

