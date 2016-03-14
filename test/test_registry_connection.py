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

ACQUA = True
try:
    import scape.registry.acqua.connection
except ImportError:
    ACQUA = False

if ACQUA:
    from scape.utils import ( singleton, memoized_property )

    @singleton
    def simple_connection():
        return scape.registry.acqua.connection.MockAcquaConnection(
            log_level='warn'
        )

    class TestConnection(TestCase):
        @property
        def connection(self):
            return simple_connection()

        def test_properties(self):
            self.assertTrue(bool(self.connection.acqua.properties))

        def test_configuration(self):
            self.assertTrue(bool(self.connection.acqua.configuration))

        def test_database(self):
            self.assertTrue(bool(self.connection.acqua.database))

        def test_db_class(self):
            self.assertEquals(
                self.connection.acqua.configuration.get('databaseClass'),
                'edu.mit.ll.acqua.database.mock.MockDatabase'
            )



if __name__=='__main__':
    unittest.main()

