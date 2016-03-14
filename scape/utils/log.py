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




import sys
import logging

def new_log(name=None):
    log = logging.getLogger(name)
    log.addHandler(logging.NullHandler())
    return log

_log = new_log('scape.utils.logging')

def lines(*args):
    args = list(args)
    for i,a in enumerate(args):
        if type(a) in {tuple}:
            args[i] = ' '.join([str(v) for v in a])
    return ('\n{}'*len(args)).format(*args)

def perr(*a,**kw):
    kw['file'] = sys.stderr
    print(*a,**kw)
    sys.stderr.flush()

class headfoot(object):
    def __init__(self,header,footer=None):
        self.header = header
        self.footer = footer if footer else '-'*len(header)
        self.head_done = False
        self.foot_done = False
    def head(self):
        if not self.head_done:
            _log.info(self.header)
            self.head_done = True
    def foot(self):
        if (not self.foot_done) and self.head_done:
            _log.info(self.footer)
            self.foot_done = True

