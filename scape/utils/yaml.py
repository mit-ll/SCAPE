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

'''YAML utilities

'''

import os
from collections import OrderedDict
from typing import *

import yaml

import scape.utils.json

def _represent_odict(dump, tag, mapping, flow_style=None):
    """Like BaseRepresenter.represent_mapping, but does not issue the sort().
    """
    value = []
    node = yaml.MappingNode(tag, value, flow_style=flow_style)
    if dump.alias_key is not None:
        dump.represented_objects[dump.alias_key] = node
    best_style = True
    if hasattr(mapping, 'items'):
        mapping = mapping.items()
    for item_key, item_value in mapping:
        node_key = dump.represent_data(item_key)
        node_value = dump.represent_data(item_value)
        if not (isinstance(node_key, yaml.ScalarNode) and not node_key.style):
            best_style = False
        if not (isinstance(node_value, yaml.ScalarNode) and not node_value.style):
            best_style = False
        value.append((node_key, node_value))
    if flow_style is None:
        if dump.default_flow_style is not None:
            node.flow_style = dump.default_flow_style
        else:
            node.flow_style = best_style
    return node

yaml.SafeDumper.add_representer(OrderedDict,
    lambda dumper, value: _represent_odict(dumper, u'tag:yaml.org,2002:map', value))

class _Loader(yaml.Loader):
    ''' OrderedDict Loader

    https://github.com/fmenabe/python-yamlordereddictloader
    '''
    def __init__(self, *args, **kwargs):
        yaml.Loader.__init__(self, *args, **kwargs)
        self.add_constructor(
            'tag:yaml.org,2002:map', type(self).construct_yaml_map)
        self.add_constructor(
            'tag:yaml.org,2002:omap', type(self).construct_yaml_map)


    def construct_yaml_map(self, node):
        data = OrderedDict()
        yield data
        value = self.construct_mapping(node)
        data.update(value)


    def construct_mapping(self, node, deep=False):
        if isinstance(node, yaml.MappingNode):
            self.flatten_mapping(node)
        else:
            raise yaml.constructor.ConstructError(None, None,
                'expected a mapping node, but found %s' % node.id,
                node.start_mark)

        mapping = OrderedDict()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError as err:
                raise yaml.constructor.ConstructError(
                    'while constructing a mapping', node.start_mark,
                    'found unacceptable key (%s)' % err, key_node.start_mark)
            value = self.construct_object(value_node, deep=deep)
            mapping[key] = value
        return mapping

    
def json_to_yaml(*paths):
    if type(paths[0]) is not str:
        paths = paths[0]
    for path in paths:
        root, fname = os.path.split(path)
        name, ext = os.path.splitext(fname)
        data = scape.utils.json.read_json(path)
        new_path = os.path.join(root, name + '.yml')
        write_yaml(data, new_path)
        
def read_yaml(path: str) -> Any:
    with open(path) as rfp:
        data = yaml.load(rfp, Loader=_Loader)
    return data

def write_yaml(data: Any, path: str) -> None:
    with open(path,'w') as wfp:
        yaml.safe_dump(data, wfp, default_flow_style=False)
