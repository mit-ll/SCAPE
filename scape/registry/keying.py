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

''' Keying helper functions
'''
import os
import sys
import csv
import hashlib
import base64
import string
import re
import json
import time
import pprint
from datetime import datetime,timedelta

SEP = '_'

def reverse_ts(ts):
    nts = datetime.fromtimestamp(-time.mktime(ts.timetuple()))
    return nts.strftime('%Y%m%d%H%M%S')

def ts_from_key(key):
    try:
        ts = key.split('_')[1]
        return ts_from_revts(ts)
    except:
        return datetime.now()

def ts_from_revts(revts):
    dt = datetime.strptime(revts,'%Y%m%d%H%M%S')
    return datetime.fromtimestamp(-time.mktime(dt.timetuple()))

def key_row(row,nshards):
    try:
        vstring = json.dumps(row,sort_keys=True)
    except:
        vstring = pprint.pformat(row)

    digest = hashlib.sha1(vstring).digest()
    H = base64.b64encode(digest)
    ns = nshards
    shardval = ord(digest[0])/256.
    shard = str(int((shardval*ns))).zfill(2)
    kts = reverse_ts(row.datetime)
    key_elements = [shard,kts,H[-10:-2]]
    return SEP.join(key_elements)

def row_key(shard,ts=None,hash=None):
    elements = [shard] + [ts] if ts else [] + [hash] if hash else []
    return SEP.join(elements)
