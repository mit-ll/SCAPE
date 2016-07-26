import re
from nose.tools import *

from scape.registry.dim import Dim, dim

# Dimension #############################################################

@raises(ValueError)
def test_dim_badarg():
    dim(1)

@raises(ValueError)
def test_dim_badarg2():
    Dim(1)

def test_dim_repr():
    d = dim('ip')
    assert_equal( d , eval(repr(d)))

def test_dim_html():
    d = dim('ip')
    d._repr_html_()

def test_parse_dims():
    assert_equal( dim(None) , None)
    assert_equal( dim('src') , Dim('src'))
    d = dim('src')
    assert_equal( dim(d) , d)

