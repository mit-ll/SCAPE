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

class Condition(object):
    type=None
    def __and__(self, other):
        return NotImplemented
    def __or__(self, other):
        return NotImplemented
    def __xor__(self, other):
        return NotImplemented
    def __invert__(self):
        return Not(self)

    @property
    def equals_conditions(self):
        raise NotImplementedError(
            "Not called on Condition subclass with equals_condition"
            " property implemented"
        )
        
    @property
    def regex_conditions(self):
        raise NotImplementedError(
            "Not called on Condition subclass with regex_condition"
            " property implemented"
        )
        
class BranchCondition(Condition):
    def __init__(self,*nodes):
        self.children = nodes

    @property
    def equals_conditions(self):
        C = []
        for c in self.children:
            C.extend(c.equals_conditions)
        return C

    @property
    def regex_conditions(self):
        C = []
        for c in self.children:
            C.extend(c.regex_conditions)
        return C

    def __and__(self,other):
        return And(self,other)
    def __or__(self,other):
        return Or(self,other)
    
class Or(BranchCondition):
    type='or'
    def __or__(self, other):
        node = self
        if isinstance(other,LeafCondition):
            node = Or(*(self.children + (other,)))
        else:
            if other.type is 'or':
                node = Or(*(self.children+other.children))
            else:
                node = Or(self,other)
        return node

class And(BranchCondition):
    type='and'
    def __and__(self, other):
        node = self
        if isinstance(other,LeafCondition):
            node = And(*(self.children + (other,)))
        else:
            if other.type is 'and':
                node = And(*(self.children+other.children))
            else:
                node = And(self,other)
        return node

class Not(BranchCondition):
    type='not'

class LeafCondition(Condition):
    def __init__(self, field, value, tagged_dim=None):
        super(LeafCondition,self).__init__()
        self.field = field
        self.value = value
        self.tagged_dim = tagged_dim

    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)

    @property
    def equals_conditions(self):
        return []

    @property
    def regex_conditions(self):
        return []

    @property
    def less_than_conditions(self):
        return []

    @property
    def greater_than_conditions(self):
        return []


class EqualsCondition(LeafCondition):
    type='equals'
    @property
    def equals_conditions(self):
        return [self]

class RegexCondition(LeafCondition):
    type='regex'
    @property
    def regex_conditions(self):
        return [self]


