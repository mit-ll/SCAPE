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

''' Wrapper for Acqua Condition objects
'''
import acqua

import scape.registry.condition as reg_condition

def resolve_where(condition):
    cmap = {
        'and': acqua.AndNode,
        'or': acqua.OrNode,
        'not': acqua.NotNode,
        'equals': acqua.EqualsCondition,
        'regex': acqua.RegexCondition,
    }
    def get_java_condition(node):
        java_class = cmap[node.type]
        if node.type in {'and','or','not'}:
            java_condition = java_class()
            for child in node.children:
                java_condition.addChild(get_java_condition(child))
        elif node.type in {'equals','regex'}:
            java_condition = java_class(node.field, node.value)
            
        return java_condition

    java_condition = get_java_condition(condition)
    return acqua.Where(java_condition)
        
