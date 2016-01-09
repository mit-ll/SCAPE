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
import unittest
from unittest import TestCase
import time
import datetime

import dateutil.parser

import scape.utils.time as utime

from scape.utils.time import (
    rdelta, dtfloor, dtceil, datetime_range,
    is_next_hour, is_next_day, is_next_month, is_next_year,
    rdt_to_secs,
    timestamp_buckets_of_max_size,
    timestamp_buckets_of_size,
    datetime_buckets_of_size, 
)

class TestDtFloor(TestCase):
    def setUp(self):
        self.now = datetime.datetime(2014,1,2,3,4,5,6)
        self.now_str = '2014-01-02 03:04:05.000006'

    def test_dt_str(self):
        self.assertEquals(dtfloor('2014-01-02 03:04:05',datetime.timedelta(hours=1)),
                          datetime.datetime(2014,1,2,3))
    def test_delta_str(self):
        self.assertEquals(dtfloor(self.now,'1s'),
                          datetime.datetime(2014,1,2,3,4,5))
    def test_both_str(self):
        self.assertEquals(dtfloor('2014-01-02 03:04:05','1h'),
                          datetime.datetime(2014,1,2,3))
        
    def test_second(self):
        self.assertEquals(dtfloor(self.now,'1s'),datetime.datetime(2014,1,2,3,4,5))
    def test_minute(self):
        self.assertEquals(dtfloor(self.now,'1m'),datetime.datetime(2014,1,2,3,4))
    def test_hour(self):
        self.assertEquals(dtfloor(self.now,'1h'),datetime.datetime(2014,1,2,3))
    def test_day(self):
        self.assertEquals(dtfloor(self.now,'1d'),datetime.datetime(2014,1,2))
    def test_week(self):
        self.assertEquals(dtfloor(self.now,'1w'),datetime.datetime(2013,12,29))
        
class TestDtCeil(TestCase):
    def setUp(self):
        self.now = datetime.datetime(2014,1,2,3,4,5,6)
        self.now_str = '2014-01-02 03:04:05.000006'

    def test_dt_str(self):
        self.assertEquals(dtceil('2014-01-02 03:04:05',datetime.timedelta(hours=1)),
                          datetime.datetime(2014,1,2,4))
    def test_delta_str(self):
        self.assertEquals(dtceil(self.now,'1s'),
                          datetime.datetime(2014,1,2,3,4,6))
    def test_both_str(self):
        self.assertEquals(dtceil('2014-01-02 03:04:05','1h'),
                          datetime.datetime(2014,1,2,4))
        
    def test_second(self):
        self.assertEquals(dtceil(self.now,'1s'),datetime.datetime(2014,1,2,3,4,6))
    def test_minute(self):
        self.assertEquals(dtceil(self.now,'1m'),datetime.datetime(2014,1,2,3,5))
    def test_hour(self):
        self.assertEquals(dtceil(self.now,'1h'),datetime.datetime(2014,1,2,4))
    def test_day(self):
        self.assertEquals(dtceil(self.now,'1d'),datetime.datetime(2014,1,3))
    def test_week(self):
        self.assertEquals(dtceil(self.now,'1w'),datetime.datetime(2014,1,5))

class TestDatetimeRange(TestCase):
    maxDiff = 2000
    def setUp(self):
        self.start = datetime.datetime(2014,5,10,13,25,27)
        self.end = datetime.datetime(2014,5,10,13,28,44)
        self.delta = datetime.timedelta(minutes=1)

        self.start_ts = '2014-05-10 13:25:27'
        self.end_ts = '2014-05-10 13:28:44'
        self.delta_ts = '1m'

        self.correct = [
            (datetime.datetime(2014, 5, 10, 13, 25),
             datetime.datetime(2014, 5, 10, 13, 26)),
            (datetime.datetime(2014, 5, 10, 13, 26),
             datetime.datetime(2014, 5, 10, 13, 27)),
            (datetime.datetime(2014, 5, 10, 13, 27),
             datetime.datetime(2014, 5, 10, 13, 28)),
            (datetime.datetime(2014, 5, 10, 13, 28),
             datetime.datetime(2014, 5, 10, 13, 29))
        ]

        self.correct_exact = [
            (datetime.datetime(2014, 5, 10, 13, 25, 27),
             datetime.datetime(2014, 5, 10, 13, 26)),
            (datetime.datetime(2014, 5, 10, 13, 26),
             datetime.datetime(2014, 5, 10, 13, 27)),
            (datetime.datetime(2014, 5, 10, 13, 27),
             datetime.datetime(2014, 5, 10, 13, 28)),
            (datetime.datetime(2014, 5, 10, 13, 28),
             datetime.datetime(2014, 5, 10, 13, 28, 44))
        ]

    def test_datetime_range(self):
        ranges = list(utime.datetime_range(
            self.start, self.end, self.delta
        ))
        for i,(start,end) in enumerate(ranges):
            self.assertEquals(start,self.correct[i][0])
            self.assertEquals(end,self.correct[i][1])

    def test_datetime_range_ts(self):
        ranges = list(utime.datetime_range(
            self.start_ts, self.end_ts, self.delta_ts
        ))
        for i,(start,end) in enumerate(ranges):
            self.assertEquals(start,self.correct[i][0])
            self.assertEquals(end,self.correct[i][1])

    def test_datetime_range_exact(self):
        ranges = list(utime.datetime_range(
            self.start, self.end, self.delta, exact=True,
        ))
        for i,(start,end) in enumerate(ranges):
            self.assertEquals(start,self.correct_exact[i][0])
            self.assertEquals(end,self.correct_exact[i][1])

    def test_datetime_range_ts(self):
        ranges = list(utime.datetime_range(
            self.start_ts, self.end_ts, self.delta_ts, exact=True
        ))
        for i,(start,end) in enumerate(ranges):
            self.assertEquals(start,self.correct_exact[i][0])
            self.assertEquals(end,self.correct_exact[i][1])

class TestIsNextFuncs(TestCase):
    def test_is_next_hour(self):
        self.assertTrue(is_next_hour(
            datetime.datetime(2014,5,10,23,),
            datetime.datetime(2014,5,11,0,59),
        ))
        self.assertFalse(is_next_hour(
            datetime.datetime(2014,5,10,23,59),
            datetime.datetime(2014,5,11,1,),
        ))
    def test_is_next_day(self):
        self.assertTrue(is_next_day(
            datetime.datetime(2014,5,10,),
            datetime.datetime(2014,5,11,23,59),
        ))
        self.assertFalse(is_next_day(
            datetime.datetime(2014,5,10,23,59),
            datetime.datetime(2014,5,12,0,),
        ))
    def test_is_next_month(self):
        self.assertTrue(is_next_month(
            datetime.datetime(2014,5,01,),
            datetime.datetime(2014,6,30,23,59),
        ))
        self.assertFalse(is_next_month(
            datetime.datetime(2014,5,30,23,59),
            datetime.datetime(2014,7,1,),
        ))
    def test_is_next_year(self):
        self.assertTrue(is_next_year(
            datetime.datetime(2014,1,1,),
            datetime.datetime(2015,12,31,23,59),
        ))
        self.assertFalse(is_next_year(
            datetime.datetime(2014,12,31,23,59),
            datetime.datetime(2016,1,1,),
        ))

class TestRdtToSecs(TestCase):
    def test_rd_to_secs(self):
        self.assertEquals(
            rdt_to_secs(datetime.datetime(2014,1,1),
                        rdelta(minutes=1)),
            60,
        )

class TestTimestampBucketsOfMaxSize(TestCase):
    def test_tsboms(self):
        start = '2014-05-10 13:57'
        end = '2014-05-11 03:05'
        correct = ['201405101357', '201405101358', '201405101359',
                   '2014051014', '2014051015', '2014051016',
                   '2014051017', '2014051018', '2014051019',
                   '2014051020', '2014051021', '2014051022',
                   '2014051023', '2014051100', '2014051101',
                   '2014051102',
                   '201405110300', '201405110301', '201405110302',
                   '201405110303', '201405110304', '201405110305']
        self.assertEquals(
            timestamp_buckets_of_max_size(start,end),
            correct,
        )
        
class TestTimestampBucketsOfSize(TestCase):
    def test_tsbos(self):
        correct = ['20140510',
                   '20140511',
                   '20140512',
                   '20140513',
                   '20140514',
                   '20140515',
                   '20140516',
                   '20140517',
                   '20140518',
                   '20140519',
                   '20140520']
        start = '2014-05-10 13:57'
        end = '10d'
        size = 'day'
        self.assertEquals(
            timestamp_buckets_of_size(start,end,size),
            correct,
        )

class TestDatetimeBucketsOfSize(TestCase):
    def test_dtbos(self):
        correct = [datetime.datetime(2014, 5, 10, 0, 0),
                   datetime.datetime(2014, 5, 11, 0, 0),
                   datetime.datetime(2014, 5, 12, 0, 0),
                   datetime.datetime(2014, 5, 13, 0, 0),
                   datetime.datetime(2014, 5, 14, 0, 0),
                   datetime.datetime(2014, 5, 15, 0, 0),
                   datetime.datetime(2014, 5, 16, 0, 0),
                   datetime.datetime(2014, 5, 17, 0, 0),
                   datetime.datetime(2014, 5, 18, 0, 0),
                   datetime.datetime(2014, 5, 19, 0, 0),
                   datetime.datetime(2014, 5, 20, 0, 0)]
        start = '2014-05-10 13:57'
        end = '10d'
        size = 'day'
        self.assertEquals(
            datetime_buckets_of_size(start,end,size),
            correct,
        )
        
if __name__=='__main__':
    unittest.main()
    
