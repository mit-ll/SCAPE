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

'''scape.registry.naming

Naming conventions for knowledge registry elements, SCAPE Accumumulo
tables, etc.

'''

def splunk2event(index):
    return index
    #return 'splunk-'+index

def event2table(e,index=None):
    tname = 'scape_'+e.replace('-','_')
    if index is not None:
        tname += '_{}'.format(index)
    return tname

PREFIX = {
    'state': 'state:',
    'event': 'event:',
    'tag': 'tag:',
    'dim': 'dim:',
    'field': 'field:',
    }

def field2node(p,f):
    # given parent name and field name, return node ID
    return PREFIX['field'] + ':'.join([p,f])

def state2node(e):
    return PREFIX['state'] + e

def event2node(e):
    return PREFIX['event'] + e

def dim2node(d):
    return PREFIX['dim'] + d

def tag2node(t):
    return PREFIX['tag'] + t

    
