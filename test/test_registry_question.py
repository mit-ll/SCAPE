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
from datetime import date, datetime, timedelta

from scape.utils import ( memoized_property )
from scape.registry.exceptions import (
    ScapeTimeError
)

class TestQuestion(TestCase):
    @memoized_property
    def registry(self):
        import test_registry_registry
        return test_registry_registry.registry_with_data()

    def test_unique(self):
        Q = self.registry.selection.question

        self.assertEquals(Q('d0').unique, {'d0': {'a': 1, 'b': 1}})
        self.assertEquals(Q('d0').sunique, {'d0': [('a', 1), ('b', 1)]})

        self.assertEquals(Q('d1').unique, {'d1': {'10': 1, '20': 2, '30':1}})
        self.assertEquals(Q('d1').sunique,
                          {'d1': [('10', 1),('30', 1), ('20', 2)]})

        self.assertEquals(Q('d2').unique, {'d2': {'c': 1, 'd': 1}})
        self.assertEquals(Q('d2').sunique,{'d2': [('c', 1), ('d', 1)] })

        self.assertEquals(
            Q('d0,d1,d2').unique,
            {
                'd0': {'a': 1, 'b': 1},
                'd1': {'10': 1, '20': 2, '30':1},
                'd2': {'c': 1, 'd': 1},
            }
        )

    def test_start(self):
        Q = self.registry.selection.question

        self.assertEquals(Q.start, Q.end - timedelta(minutes=15))
        self.assertEquals(Q.start, datetime(2014,1,1,10,15,1))

        Q10m = Q(start='-10m')
        self.assertEquals(Q10m.start, Q10m.end - timedelta(minutes=10))
        Q10m = Q(start=timedelta(minutes=10))
        self.assertEquals(Q10m.start, Q10m.end - timedelta(minutes=10))

        Q1d = Q(start='-1d')
        self.assertEquals(Q1d.start, Q1d.end - timedelta(days=1))
        Q1d = Q(start=timedelta(days=1))
        self.assertEquals(Q1d.start, Q1d.end - timedelta(days=1))

        Qd = Q(start='2013-12-01 10:00')
        self.assertEquals(Qd.start, datetime(2013,12,1,10))
        Qd = Q(start=datetime(2013,12,1,10))
        self.assertEquals(Qd.start, datetime(2013,12,1,10))
        Qd = Q(start=date(2013,12,1))
        self.assertEquals(Qd.start, datetime(2013,12,1))

        with self.assertRaises(ScapeTimeError):
            Q(start=30)

    def test_end(self):
        Q = self.registry.selection.question

        data_now = self.registry.selection.last_time

        self.assertEquals(Q.end, data_now)
        self.assertEquals(Q.end, datetime(2014,1,1,10,30,1))

        Qm10m = Q(end='-10m')
        self.assertEquals(Qm10m.end, data_now - timedelta(minutes=10))
        Qm10m = Q(end= -timedelta(minutes=10))
        self.assertEquals(Qm10m.end, data_now - timedelta(minutes=10))

        Qp10m = Q(end= timedelta(minutes=10))
        self.assertEquals(Qp10m.end, data_now + timedelta(minutes=10))

        Qe0 = Q(end=self.registry.selection.events('e0'))
        self.assertEquals(Qe0.end, datetime(2014,1,1,10,15,1))

        Q1d = Q(end='-1d')
        self.assertEquals(Q1d.end, data_now - timedelta(days=1))
        Q1d = Q(end= -timedelta(days=1))
        self.assertEquals(Q1d.end, data_now - timedelta(days=1))

        Qd = Q(end='2013-12-01 10:00')
        self.assertEquals(Qd.end, datetime(2013,12,1,10))
        Qd = Q(end=datetime(2013,12,1,10))
        self.assertEquals(Qd.end, datetime(2013,12,1,10))
        Qd = Q(end=date(2013,12,1))
        self.assertEquals(Qd.end, datetime(2013,12,1))

        with self.assertRaises(ScapeTimeError):
            Q(end=30)

    def test_center(self):
        Q = self.registry.selection.question

        start,end = Q(center='-5m').time_window
        self.assertEqual(start, datetime(2014,1,1,10,10,1))
        self.assertEqual(end, datetime(2014,1,1,10,25,1))

        start,end = Q(start='-30m',center='-5m').time_window
        self.assertEqual(start, datetime(2014,1,1,9,55,1))
        self.assertEqual(end, datetime(2014,1,1,10,25,1))

        start,end = Q(start='-30m',center='2014-01-01 08:30').time_window
        self.assertEqual(start, datetime(2014,1,1,8,15))
        self.assertEqual(end, datetime(2014,1,1,8,45))
    
    def test_call_tagged_dim(self):
        Q = self.registry.selection.question(
            start='-30m',
            end='1s'            # XXXXX needs fix!
        )

        self.assertEquals( len(Q('d0=a').events), 1 )
        self.assertEquals( len(Q('d0=b').events), 1 )
        self.assertEquals( len(Q('d0=(a,b)').events), 2 )

        self.assertEquals( len(Q('d1=10').events), 1 )

        self.assertEquals( len(Q('d1=(10,20)').events), 3 )

        # e0:f1 (values 10, 20) and e1:f0 (values 20, 30)
        self.assertEquals( len(Q('d1=(10,20,30)').events), 4 )
        # same, since e0:f1 and e1:f0 both share tag t2
        self.assertEquals( len(Q('t2:d1=(10,20,30)').events), 4 )

        # e0:f1 (values 10, 20)
        self.assertEquals( len(Q('t1:d1=(10,20,30)').events), 2 )

        # e1:f0 (values 20, 30)
        self.assertEquals( len(Q('t3:d1=(10,20,30)').events), 2 )

        self.assertEquals( len(Q('d2=c').events), 1 )
        self.assertEquals( len(Q('d2=d').events), 1 )
        self.assertEquals( len(Q('d2=(c,d)').events), 2 )

        self.assertEquals( Q('t1:=a').events['d1'], [('10',)] )
        self.assertEquals( Q('d1=30').events['d2'], [('d',)] )
        
    def test_call_field(self):
        Q = self.registry.selection.question(
            start='-30m',
        )

        # e0:f0
        self.assertEquals( len(Q('@f0=a').events), 1 )
        # e1:f0
        self.assertEquals( len(Q('@f0=20').events), 1 )
        # e1:f0
        self.assertEquals( len(Q('@f0=30').events), 1 )

        self.assertEquals( Q('@f0=a').events[0]['@f1'], ('10',) )

    def test_call_and(self):
        Q = self.registry.selection.question(
            start='-30m',
        )

        self.assertEquals( len(Q('d0=a & d1=20').events), 0 )
        self.assertEquals( len(Q('d0=b & d1=20').events), 1 )

        self.assertEquals( Q('d0=b, d1=20').events['@scape_rowid'],
                           Q('d0=b & d1=20').events['@scape_rowid'] )
        
        self.assertEquals( Q('d0=b & d1=20').events['@scape_rowid'],
                           (Q('d0=b')&Q('d1=20')).events['@scape_rowid'] )

    def test_call_or(self):
        Q = self.registry.selection.question(
            start='-30m',
        )

        # Due to insufficiency of Question parsing, all tagged
        # dimensions used in a particular Question must be present in
        # an Event for the Question to be valid and resolvable. Thus
        # the below 2 Questions aren't strictly correct
        #
        # This would be 3, but Event e1 does not contain Dimensions d0
        self.assertEquals( len(Q('d0=a | d1=20').events), 2 ) 
        # This would be 2, for the same reason
        self.assertEquals( len(Q('d0=b | d1=20').events), 1 )
        #
        # Suffice to say, this should be fixed in later releases

        self.assertEquals( Q('d0=b | d1=20').events['@scape_rowid'],
                           (Q('d0=b') | Q('d1=20')).events['@scape_rowid'] )

    def test_call_not(self):
        Q = self.registry.selection.question(
            start='-30m',
        )

        self.assertEquals( len(Q('d0!=b').events), 1 )
        self.assertEquals( Q('d0!=b').events['@scape_rowid'],
                           (~Q('d0=b')).events['@scape_rowid'] )
        

if __name__=='__main__':
    unittest.main()
    
