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

from scape.registry.registry import Registry

from weblog_data_source import get_weblog_ds, get_auth_ds

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
    act = r.has_any('@NON_EXISTENT_FIELD')
    assert_equal(0, len(act))

def test_registry_has_html():
    r = Registry({'testds':ds})
    act = r.has_any('client:ip,url')
    assert_equal(1, len(act))
    act._repr_html_()

def test_registry_has_all():
    r = Registry({'testds':ds})
    act = r.has_all('client:ip,url')
    assert_equal(1, len(act))
    act._repr_html_()

def test_registry_has_all_halffound():
    r = Registry({'testds':ds})
    act = r.has_all('client:ip,url,nonexistent_dim')
    assert_equal(0, len(act))
    act._repr_html_()

def test_registry_alL_fields_html():
    r = Registry({'testds':ds, 'auths': get_auth_ds()})
    r.fields._repr_html_()


def test_registry_tags():
    r = Registry({'testds':ds, 'auths': get_auth_ds()})
    assert_equal(set(['supplied', 'dst', 'client', 'server', 'http']), r.tags)

def test_registry_dims():
    r = Registry({'testds':ds, 'auths': get_auth_ds()})
    assert_equal(set(['ip','url','status_code','sec','user','ip','fqdn','sec']), r.dims)
