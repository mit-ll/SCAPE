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

