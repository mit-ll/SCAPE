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

