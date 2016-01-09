"""
Copyright (2016) Massachusetts Institute of Technology.  Reproduction/Use 
of all or any part of this material shall acknowledge the MIT Lincoln 
Laboratory as the source under the sponsorship of the US Air Force 
Contract No. FA8721-05-C-0002.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
import pprint
import unittest
from unittest import TestCase

from scape.utils import ( singleton, memoized_property )

import scape.registry.registry

REGISTRY = {
    "name": "UnitTest",

    "events" : {
        "e0": {
            "table": "scape_e0",
            "family": "raw",
            "time": "ts",

            "fields": {
                "ts": {
                    "dim": "datetime",
                },
                "f0": {
                    "dim": "d0",
                    "tags": ["t0","t1"],
                },
                "f1": {
                    "dim": "d1",
                    "tags": ["t1","t2"],
                },
            },
        },

        "e1": {
            "time": "timestamp",

            "fields": {
                "timestamp": {
                    "dim": "datetime",
                },
                "f0": {
                    "dim": "d1",
                    "tags": ["t2","t3"],
                    },
                "f1": {
                    "dim": "d2",
                    "tags": ["t3","t4"],
                },
            },
        },
    },
}

DATA = {
    'e0': [
        {'ts': '2014-01-01 10:15:00',
         'f0': 'a',
         'f1': '10'},
        {'ts': '2014-01-01 10:15:01',
         'f0': 'b',
         'f1': '20'},
    ],
    'e1': [
        {'timestamp': '2014-01-01 10:30:00',
         'f0': '20',
         'f1': 'c'},
        {'timestamp': '2014-01-01 10:30:01',
         'f0': '30',
         'f1': 'd'},
    ]
}

@singleton
def registry():
    import test_registry_connection
    connection = test_registry_connection.simple_connection()

    return scape.registry.registry.Registry(
        registry_dict=REGISTRY, connection=connection,
    )

@singleton
def registry_with_data():
    R = registry()
    for T in R.selection.tabular:
        name = T.node['name']
        T.create_tables()
        T.ingest_rows(DATA[name])
    return R

class TestRegistry(TestCase):
    @memoized_property
    def registry(self):
        return registry_with_data()

    def test_creation(self):
        self.assertTrue(bool(self.registry))

    def test_connection_existence(self):
        self.assertTrue(bool(self.registry.connection))

    def test_selection_existence(self):
        self.assertTrue(bool(self.registry.selection))

    def test_question_existence(self):
        self.assertTrue(bool(self.registry.question))

    def test_registry_dict(self):
        self.assertEquals(len(self.registry.registry_dict),2)
        self.assertEquals(
            set(self.registry.registry_dict.keys()),
            {'name','events'}
        )

    def test_graph_existence(self):
        graph = self.registry.graph
        self.assertTrue(bool(graph))

    def test_graph_nodes(self):
        graph = self.registry.graph
        self.assertEquals(
            len(graph.nodes()),
            ( 2 +               # events (e0, e1)
              6 +               # fields ( e0:ts, e0:f0, e0:f1,
                                #          e1:timestamp, e1:f0, e1:f1 )
              4 +               # dims (datetime,d0,d1,d2)
              5 +               # tags (t0, t1, t2, t3, t4)
              1                 # root node
              ),
            # pprint.pformat(graph.nodes())
        )

    def test_graph_edges(self):
        graph = self.registry.graph
        self.assertEquals(
            len(graph.edges()),
            ( 8 +               # (fields -> tags)
              6 +               # (events -> fields)
              6 +               # (fields -> dims)
              17                # (root -> all nodes)
              ),
            # pprint.pformat(graph.edges())
        )




if __name__=='__main__':
    unittest.main()

