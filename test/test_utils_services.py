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

import scape.utils.services as services

class TestPort2Service(TestCase):
    def test_p2s(self):
        p2s = services.Port2Service()

        self.assertEquals(p2s[22],('ssh',))
        self.assertEquals(p2s['22'],('ssh',))

        names = p2s[22]
        self.assertEquals(
            names[0].protocols,
            {'sctp': ['SSH'],
             'tcp': ['The Secure Shell (SSH) Protocol'],
             'udp': ['The Secure Shell (SSH) Protocol']},
        )

        p2s = services.Port2Service(simplified=False)

        self.assertEquals(
            p2s[22],
            [('ssh', 'The Secure Shell (SSH) Protocol', 'tcp'),
             ('ssh', 'The Secure Shell (SSH) Protocol', 'udp'),
             ('ssh', 'SSH', 'sctp')],
        )

        
if __name__=='__main__':
    unittest.main()
    
