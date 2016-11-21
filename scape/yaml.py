# Copyright (2016) Massachusetts Institute of Technology.
# Reproduction/Use of all or any part of this material shall
# acknowledge the MIT Lincoln Laboratory as the source under the
# sponsorship of the US Air Force Contract No. FA8721-05-C-0002.

# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

'''YAML utilities

'''

import os
from collections import OrderedDict

import ruamel.yaml as yaml

def dump(*a, **kw):
    kw['Dumper'] = yaml.RoundTripDumper
    kw['default_flow_style'] = False
    return yaml.dump(*a, **kw)

def load(*a, **kw):
    kw['Loader'] = yaml.RoundTripLoader
    return yaml.load(*a, **kw)
    
def read_yaml(path):
    with open(path) as rfp:
        data = load(rfp)
    return data

def write_yaml(data, path):
    with open(path,'w') as wfp:
        dump(data, wfp)

