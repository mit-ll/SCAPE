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

import scape.utils.ip as ip

class TestIpFuncs(TestCase):
    def test_is_ip(self):
        self.assertTrue(ip.is_ip('1.2.3.4'))
        self.assertTrue(ip.is_ip(ip.random_ip()))
        self.assertFalse(ip.is_ip('1.2.3.4.5'))
        self.assertFalse(ip.is_ip('256.1.1.1'))

    def test_ip2num(self):
        self.assertEqual(ip.ip2num('0.0.0.0'),0)
        self.assertEqual(ip.ip2num('0.0.0.1'),1<<0)
        self.assertEqual(ip.ip2num('0.0.1.0'),1<<8)
        self.assertEqual(ip.ip2num('0.1.0.0'),1<<16)
        self.assertEqual(ip.ip2num('1.0.0.0'),1<<24)
        self.assertEqual(ip.ip2num('255.255.255.255'),2**32-1)
        self.assertEqual(
            ip.ip2num('23.35.202.55')-ip.ip2num('23.35.202.54'),
            1
        )

    def test_num2ip(self):
        self.assertEqual(ip.num2ip(0),'0.0.0.0')
        self.assertEqual('0.0.0.1',ip.num2ip(1<<0))
        self.assertEqual('0.0.1.0',ip.num2ip(1<<8))
        self.assertEqual('0.1.0.0',ip.num2ip(1<<16))
        self.assertEqual('1.0.0.0',ip.num2ip(1<<24))
        self.assertEqual(ip.num2ip(2**32-1),'255.255.255.255')


    def test_sort_ips(self):
        ips = ['1.2.3.4','10.2.3.4','2.3.4.5']
        self.assertEqual(
            ip.sort_ips(ips),['1.2.3.4','2.3.4.5','10.2.3.4'],
        )
        self.assertEqual(
            sorted(ips),['1.2.3.4','10.2.3.4','2.3.4.5'],
        )


if __name__=='__main__':
    unittest.main()
        
