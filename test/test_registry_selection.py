# Copyright (2016) Massachusetts Institute of Technology.  Reproduction/Use 
# of all or any part of this material shall acknowledge the MIT Lincoln 
# Laboratory as the source under the sponsorship of the US Air Force 
# Contract No. FA8721-05-C-0002.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from unittest import TestCase
from datetime import datetime

from scape.utils import ( memoized_property )

class TestRegistry(TestCase):
    @memoized_property
    def registry(self):
        from . import test_registry_registry
        return test_registry_registry.registry_with_data()

    def test_events(self):
        S = self.registry.selection
        
        self.assertEqual(S.events.names,{'e0','e1'})

        self.assertEqual(S.events('e0').fields.names,{'ts','f0','f1'})
        self.assertEqual(S.events('e0').node['table'],'scape_e0')
        self.assertEqual(S.events('e0').node['family'],'raw')
        self.assertEqual(S.events('e0').node['time'],'ts')
                          
        self.assertEqual(S.events('e1').fields.names,{'timestamp','f0','f1'})
        # If table name is not provided it should create a default one
        self.assertEqual(S.events('e1').node['table'],'scape_e1')
        # If family is not provided it defaults to "raw"
        self.assertEqual(S.events('e1').node['family'],'raw')
        self.assertEqual(S.events('e1').node['time'],'timestamp')

    def test_events_have(self):
        S = self.registry.selection
        
        # e0:f0
        self.assertEqual(len(S.events.have('t0:d0')), 1)
        # e0:f1
        self.assertEqual(len(S.events.have('t1:d1')), 1)

        # e0:f1 and e1:f0
        self.assertEqual(len(S.events.have('t2:')), 2)
        self.assertEqual(len(S.events.have('d1')), 2)
        self.assertEqual(len(S.events.have('t2:d1')), 2)

        # e1:f0
        self.assertEqual(len(S.events.have('t3:d1')), 1)
        # e1:f1
        self.assertEqual(len(S.events.have('t3:d2')), 1)

        self.assertEqual(len(S.events.have('t1:t2:t3')), 0)

    def test_fields(self):
        S = self.registry.selection

        # Since the Selection.names property returns a set of unique
        # Field names and Events can have Fields with the same name,
        # we have to use the sorted attribute selection by name to
        # test
        self.assertEqual(sorted(S.fields['name']),
                          ['f0','f0','f1','f1','timestamp','ts'])

        self.assertEqual(S.fields.events.names, {'e0','e1'})
        self.assertEqual(S.fields.dims.names, {'d0','d1','d2','datetime'})
        self.assertEqual(S.fields.tags.names, {'t0','t1','t2','t3','t4'})

    def test_fields_have(self):
        S = self.registry.selection
        
        self.assertEqual(len(S.dims('datetime').fields), 2)

        # e0:f0
        self.assertEqual(len(S.fields.have('t0:d0')), 1)
        # e0:f1
        self.assertEqual(len(S.fields.have('t1:d1')), 1)

        # e0:f1 and e1:f0
        self.assertEqual(len(S.fields.have('t2:')), 2)
        self.assertEqual(len(S.fields.have('d1')), 2)
        self.assertEqual(len(S.fields.have('t2:d1')), 2)

        # e1:f0
        self.assertEqual(len(S.fields.have('t3:d1')), 1)
        # e1:f1
        self.assertEqual(len(S.fields.have('t3:d2')), 1)

        self.assertEqual(len(S.fields.have('t1:t2:t3')), 0)

    def test_dims(self):
        S = self.registry.selection

        self.assertEqual(S.dims.names, {'d0','d1','d2','datetime'})
        self.assertEqual(S.dims.fields.events.names, {'e0','e1'})
        self.assertEqual(sorted(S.dims.fields['name']),
                          ['f0','f0','f1','f1','timestamp','ts'])
        self.assertEqual(S.dims.fields.tags.names, {'t0','t1','t2','t3','t4'})

    def test_dims_have(self):
        S = self.registry.selection
        
        # e0:f0
        self.assertEqual(len(S.dims.have('t0:')), 1)

        # e0:f1 (d0) and e1:f0 (d1)
        self.assertEqual(len(S.dims.have('t1:')), 2)

        # e0:f1 and e1:f0 (both d1)
        self.assertEqual(len(S.dims.have('t2:')), 1)

        # e1:f0 (d1) and e1:f1 (d2)
        self.assertEqual(len(S.dims.have('t3:')), 2)

        self.assertEqual(len(S.dims.have('t0:t1:')), 1)
        self.assertEqual(len(S.dims.have('t1:t2:')), 1)
        self.assertEqual(len(S.dims.have('t2:t3:')), 1)
        self.assertEqual(len(S.dims.have('t3:t4:')), 1)

        self.assertEqual(len(S.dims.have('t1:t2:t3')), 0)

    def test_tags(self):
        S = self.registry.selection

        self.assertEqual(S.tags.names, {'t0','t1','t2','t3','t4'})
        self.assertEqual(S.tags.fields.events.names, {'e0','e1'})
        self.assertEqual(sorted(S.tags.fields['name']),
                          ['f0','f0','f1','f1'])
        self.assertEqual(S.tags.fields.dims.names, {'d0','d1','d2'})

    def test_tags_have(self):
        S = self.registry.selection
        
        # e0:f0 (t0,t1)
        self.assertEqual(len(S.tags.have('d0')), 2)

        # e0:f1 (t1,t2) and e1:f0 (t2,t3)
        self.assertEqual(len(S.tags.have('d1')), 3)

        # e1:f1 (t3,t4)
        self.assertEqual(len(S.tags.have('d2')), 2)

    def test_null_selection(self):
        S = self.registry.selection

        self.assertEqual(len(S.null_selection()),0)

    def test_selection_hash(self):
        S = self.registry.selection

        F0 = S.fields.have('t0:t1:')
        F1 = S.dims.have('t1:t0:').fields
        self.assertFalse( F0 is F1 )
        self.assertTrue( F0 == F1 )
        self.assertEqual(hash(F0), hash(F1))
        self.assertEqual(len( {F0, F1} ), 1)

        F = S.fields.have('t0:t1:')
        Fcopy = F()
        self.assertFalse( F is Fcopy )
        self.assertTrue( F == Fcopy )
        self.assertEqual(hash(F), hash(Fcopy))
        self.assertEqual(len( {F, Fcopy} ), 1)

    def test_tabular(self):
        S = self.registry.selection

        self.assertEqual( S.dims('d1').tabular.names, {'e0','e1'} )
        self.assertEqual( S.events('e0').tabular.names, {'e0'} )
        self.assertEqual( S.tags('t3').tabular.names, {'e1'} )
        self.assertEqual( S.tags('t3').tabular, S.have('t4:').tabular )

    def test_last_time(self):
        S = self.registry.selection

        self.assertEqual(S.last_time, datetime(2014,1,1,10,30,1))
        self.assertEqual(S.events('e0').last_time, datetime(2014,1,1,10,15,1))
        self.assertEqual(S.last_time, S.events('e1').last_time)

    def test_rebuild_connections(self):
        self.test_last_time()
        from . import test_registry_registry
        test_registry_registry.registry._value = None
        test_registry_registry.registry_with_data._value = None
        self._registry = None
        self.test_last_time()

    def test_or(self):
        S = self.registry.selection

        self.assertEqual(S.events('e0') | S.events('e1'), S.events)
            
    def test_and(self):
        S = self.registry.selection

        self.assertEqual(S.tags('t0','t1') & S.tags('t1','t2'),
                          S.tags('t1'))

    def test_sub(self):
        S = self.registry.selection

        self.assertEqual(S.tags('t0','t1') - S.tags('t1','t2'),
                          S.tags('t0'))

    def test_invert(self):
        pass

    def test_getitem(self):
        S = self.registry.selection
        self.assertEqual(S.events['family'], ['raw']*len(S.events))

    def test_get(self):
        S = self.registry.selection
        self.assertEqual(S.events.get('badkey','oops'),
                          ['oops']*len(S.events))

    def test_nonzero(self):
        S = self.registry.selection

        self.assertTrue(S)
        self.assertFalse(S.events('badevent'))
        self.assertTrue(S.events('badevent','e0'))

    def test_types(self):
        S = self.registry.selection

        self.assertEqual(S.events.type, 'event')
        self.assertEqual(S.fields.type, 'field')
        self.assertEqual(S.dims.type, 'dim')
        self.assertEqual(S.tags.type, 'tag')
        self.assertEqual((S.dims|S.tags).type, 'mixed')

        self.assertEqual(S.null_selection().type, 'null')
        self.assertEqual(S.states.type, 'null')

if __name__=='__main__':
    unittest.main()

