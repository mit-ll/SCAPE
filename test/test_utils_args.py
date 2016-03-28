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

import os
import tempfile
import shutil
import argparse
import unittest
from unittest import TestCase
from datetime import datetime, timedelta

import dateutil.parser

import scape.utils.args as args

class TestExistingPath(TestCase):
    temp_dir = ''
    temp_file_path = ''
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp('test_utils_args')
        self.temp_file_path = os.path.join(self.temp_dir,'temp_file')
        with open(self.temp_file_path,'a'):
            pass

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_good_path(self):
        path_test = args.existing_path('bad path {path}')
        self.assertEqual(path_test(self.temp_file_path),
                          self.temp_file_path)

    def test_bad_path(self):
        bad_path = os.path.join(self.temp_dir,'bad_path')
        path_test = args.existing_path('bad path {path}')

        self.assertRaises(argparse.ArgumentTypeError,path_test,bad_path)

class TestDeltaConvert(TestCase):
    def convert(self,*a,**kw):
        return args.delta_convert(*a,**kw)

    def test_bin_sizes_positive(self):
        # seconds
        self.assertEqual(self.convert('1s'),timedelta(seconds=1))
        # minutes
        self.assertEqual(self.convert('1m'),timedelta(minutes=1))
        # hours
        self.assertEqual(self.convert('1h'),timedelta(hours=1))
        # days
        self.assertEqual(self.convert('1d'),timedelta(days=1))
        # weeks
        self.assertEqual(self.convert('1w'),timedelta(weeks=1))

    def test_bin_sizes_negative(self):
        # seconds
        self.assertEqual(self.convert('-1s'),timedelta(seconds=-1))
        # minutes
        self.assertEqual(self.convert('-1m'),timedelta(minutes=-1))
        # hours
        self.assertEqual(self.convert('-1h'),timedelta(hours=-1))
        # days
        self.assertEqual(self.convert('-1d'),timedelta(days=-1))
        # weeks
        self.assertEqual(self.convert('-1w'),timedelta(weeks=-1))

    def test_positive_composite(self):
        # all
        self.assertEqual(self.convert('1s1m1h1d1w'),
                          timedelta(seconds=1,minutes=1,hours=1,
                                    days=1,weeks=1))
        # all, order-invariant
        self.assertEqual(self.convert('1w1m1d1h1s'),
                          timedelta(seconds=1,minutes=1,hours=1,
                                    days=1,weeks=1))

    def test_negative_composite_single_sign(self):
        # all
        self.assertEqual(self.convert('-1s1m1h1d1w'),
                          timedelta(seconds=-1,minutes=-1,hours=-1,
                                    days=-1,weeks=-1))
        # all, order-invariant
        self.assertEqual(self.convert('-1w1m1d1h1s'),
                          timedelta(seconds=-1,minutes=-1,hours=-1,
                                    days=-1,weeks=-1))

    def test_negative_composite_multiple_signs(self):
        # all
        self.assertEqual(self.convert('-1s-1m-1h-1d-1w'),
                          timedelta(seconds=-1,minutes=-1,hours=-1,
                                    days=-1,weeks=-1))
        # all, order-invariant
        self.assertEqual(self.convert('-1w-1m-1d-1h-1s'),
                          timedelta(seconds=-1,minutes=-1,hours=-1,
                                    days=-1,weeks=-1))

    def test_mixed_composite(self):
        # all
        self.assertEqual(self.convert('-1s1m-1h1d-1w'),
                          timedelta(seconds=-1,minutes=1,hours=-1,
                                    days=1,weeks=-1))
    
    def test_none(self):
        self.assertEqual(self.convert(None),None)

    def test_delta(self):
        minute = timedelta(minutes=1)
        self.assertEqual(self.convert(minute),minute)

    def test_zeros(self):
        now = datetime.now()
        self.assertEqual(now + self.convert('0s0m0h0d0w'),now)
        

class TestDateConvert(TestDeltaConvert):
    def convert(self,*a,**kw):
        return args.date_convert(*a,**kw)

    def test_odd_formats(self):
        odd_formats = args.odd_date_formats()
        now = datetime.now()
        for fmt in odd_formats:
            ts = now.strftime(fmt)
            dt = datetime.strptime(ts,fmt)
            self.assertEqual(self.convert(ts),dt)

    def test_standard_timestamps(self):
        ts = '2013-03-05 12:45:25'
        dt = dateutil.parser.parse(ts)
        self.assertEqual(self.convert(ts),dt)

class TestParseTimes(TestCase):
    def test_start_end_none(self):
        start,end = None,None
        self.assertEqual(args.parse_times(start,end),(start,end))

    def test_start_dt_end_dt(self):
        start = datetime.now()
        end = start + timedelta(minutes=1)
        self.assertEqual(args.parse_times(start,end),(start,end))

    def test_start_dt_end_delta(self):
        start = datetime.now()
        end = timedelta(minutes=1)
        self.assertEqual(args.parse_times(start,end),(start,start+end))

    def test_end_dt_start_delta(self):
        start = timedelta(minutes=1)
        end = datetime.now()
        self.assertEqual(args.parse_times(start,end),(end - start,end))

    def test_start_dt_end_dt_str(self):
        start = '2014-05-01T10:25:11'
        end = '2014-05-01T10:30:00'
        start_dt,end_dt = list(map(dateutil.parser.parse,[start,end]))
        self.assertEqual(args.parse_times(start,end),(start_dt,end_dt))

    def test_start_dt_end_delta_str(self):
        start = '2014-05-01T10:25:11'
        start_dt = dateutil.parser.parse(start)
        end = '1m'
        end_dt = start_dt + timedelta(minutes=1)
        self.assertEqual(args.parse_times(start,end),(start_dt,end_dt))

    def test_end_dt_start_delta_str(self):
        end = '2014-05-01T10:25:11'
        end_dt = dateutil.parser.parse(end)
        start = '1m'
        start_dt = end_dt - timedelta(minutes=1)
        self.assertEqual(args.parse_times(start,end),(start_dt,end_dt))


if __name__=='__main__':
    unittest.main()
        
