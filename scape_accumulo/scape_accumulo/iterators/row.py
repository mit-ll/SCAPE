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

from .iterator import BaseIterator

class WholeRowIterator(BaseIterator):
    classname = 'org.apache.accumulo.core.iterators.user.WholeRowIterator'

    class _row_dict(dict): pass

    @staticmethod
    def decode_row(cell):
        value = StringIO.StringIO(cell.value)
        numKeys = struct.unpack('!i',value.read(4))[0]
        row = WholeRowIterator._row_dict()
        row.key = cell.row
        for i in range(numKeys):
            cf = value.read(struct.unpack('!i',value.read(4))[0])
            fdict = row.setdefault(cf,{})
            cq = value.read(struct.unpack('!i',value.read(4))[0])
            cv = value.read(struct.unpack('!i',value.read(4))[0])
            cts = struct.unpack('!q',value.read(8))[0]/1000.
            val = value.read(struct.unpack('!i',value.read(4))[0])
            fdict.setdefault(cq,val)
        return row

class EventWholeRowIterator(BaseIterator):
    ''' EventWholeRowIterator from the QueryExecutor '''
    classname = "llcysa.queryexecutor.operator.accumuloimpl.EventWholeRowIterator"
    def __init__(self,tree=None,**kw):
        super(EventWholeRowIterator,self).__init__(**kw)

        self.tree = tree

    @property
    def properties(self):
        if not self.tree:
            return {}

        settings = {}
        def serialize(node,depth,settings,N):
            if node.get('children'):
                children = []
                for c in node['children']:
                    N = serialize(c,depth+1,settings,N)
                    children.append(N)
                N += 1
                node_name = "node{}".format(N)
                settings[node_name+'_leaf'] = "0"
                settings[node_name+'_children'] = ','.join(map(str,children))
                settings[node_name+'_class'] = node['class']
            else:
                N += 1
                node_name = "node{}".format(N)
                settings[node_name+'_leaf'] = "1"
                settings[node_name+'_arg1'] = node['args'][0]
                settings[node_name+'_arg2'] = node['args'][1]
                settings[node_name+'_class'] = node['class']
            return N
                

        count = serialize(self.tree,1,settings,0)
        settings['nodes'] = str(count)
        return settings

class Condition(dict):
    classname = ''
    def __init__(self):
        self['class'] = self.classname

class BranchCondition(Condition):
    def __init__(self):
        super(BranchCondition,self).__init__()
        self['children'] = []

    def add_child(self,child):
        self['children'].append(child)
        return child

    def add_leaf(self,cls,*a):
        child = cls(*a)
        return self.add_child(child)

    def add_equals(self,arg1,arg2):
        return self.add_leaf(EqualsCondition,arg1,arg2)
    def add_regex(self,arg1,arg2):
        return self.add_leaf(RegexCondition,arg1,arg2)
        
    def add_branch(self,cls,*a):
        child = cls(*a)
        return self.add_child(child)
        
    def add_and(self):
        return self.add_branch(AndNode)
    def add_or(self):
        return self.add_branch(OrNode)
    def add_not(self):
        return self.add_branch(NotNode)

class OrNode(BranchCondition):
    classname = 'llcysa.queryexecutor.operator.condition.OrNode'
class AndNode(BranchCondition):
    classname = 'llcysa.queryexecutor.operator.condition.AndNode'
class NotNode(BranchCondition):
    classname = 'llcysa.queryexecutor.operator.condition.NotNode'

class LeafCondition(Condition):
    def __init__(self,*args):
        super(LeafCondition,self).__init__()
        self['args'] = args

class EqualsCondition(LeafCondition):
    classname = 'llcysa.queryexecutor.operator.condition.EqualsCondition'
class RegexCondition(LeafCondition):
    classname = 'llcysa.queryexecutor.operator.condition.RegexCondition'
