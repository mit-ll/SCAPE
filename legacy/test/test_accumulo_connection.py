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
#!/usr/bin/env python

import unittest

import scape.registry.accumulo as accumulo
import scape.config as config
import scape.registry.accumulo.connection as connection

from mock import Mock

class ConnectionTest(unittest.TestCase):
    def test_connection_class_defaults(self):
        for attr in ['host','user','password']:
            self.assertEquals(config.config['accumulo']['proxy'][attr],
                              getattr(connection.Connection,attr))

    def test_connection_instance_defaults(self):
        C = connection.Connection()
        for attr in ['host','user','password']:
            self.assertEquals(config.config['accumulo']['proxy'][attr],
                              getattr(C,attr))

    def test_connection_instance_nondefault(self):
        args = 'a',10,'c','d'
        C = connection.Connection(*args)
        for i,attr in enumerate(['host','port','user','password']):
            self.assertEquals(args[i],getattr(C,attr))
        
    def test_list_tables(self):
        conn = connection.Connection()
        conn.client = Mock()
        conn.login = "Login"
        conn.client.listTables = Mock()
        conn.client.listTables.return_value = set(["t1", "t2", "t3"])

        res = conn.tables
        conn.client.listTables.assert_called_with("Login")
        self.assertEquals(set(["t1", "t2", "t3"]), set(res))

    def test_connected(self):
        conn = connection.Connection()
        conn.transport = Mock()
        conn.transport.isOpen = Mock()
        conn.transport.isOpen.return_value = True

        res = conn.connected
        self.assertEquals(True, res)

#----------------------------------------------

def main():
    unittest.main()

if __name__ == '__main__':
    main()
