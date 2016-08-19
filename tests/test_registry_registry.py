import re

from nose.tools import *

from scape.registry.registry import Registry

from weblog_data_source import get_weblog_ds

ds = get_weblog_ds()

# Registry #############################################################

def test_registry():
    r = Registry({'testds':ds})

def test_html():
    r = Registry({'testds':ds})
    r._repr_html_()

def test_registry_has():
    r = Registry({'testds':ds})
    act = r.has('@clientip')
    assert_equal(1, len(act))
    assert_equal(ds, act['testds'])

def test_registry_has_nofield():
    r = Registry({'testds':ds})
    act = r.has('@NON_EXISTENT_FIELD')
    assert_equal(0, len(act))

def test_registry_has_html():
    r = Registry({'testds':ds})
    act = r.has('client:ip,url')
    assert_equal(1, len(act))
    act._repr_html_()


