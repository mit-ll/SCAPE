"""
Copyright (2016) Massachusetts Institute of Technology.  Reproduction/Use 
of all or any part of this material shall acknowledge the MIT Lincoln 
Laboratory as the source under the sponsorship of the US Air Force 
Contract No. FA8721-05-C-0002.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
from .iterator import BaseIterator
from ..utils import transform_columns as _transform_columns

class BaseCombiner(BaseIterator):
    """docstring for BaseCombiner"""
    def __init__(self, columns=[], combine_all_columns=True,
                 encoding_type="STRING", **kw):

        super(BaseCombiner, self).__init__(**kw)

        self.columns = columns
        self.encoding_type = encoding_type

        if len(columns) == 0:
            self.combine_all_columns = combine_all_columns
        else:
            self.combine_all_columns = False

    _columns = None
    @property
    def columns(self):
        return _transform_columns(self._columns)
    @columns.setter
    def columns(self,columns):
        self._columns = columns

    def add_column(self, colf, colq=None):
        self.combine_all_columns = False
        if colq:
            self.columns.append([colf, colq])
        else:
            self.columns.append([colf])

    @property
    def properties(self):
        def encode_column(col):
            return col[0] if len(col) == 1 else ":".join(col)
        return {
            "type":self.encoding_type,
            "all": str(self.combine_all_columns).lower(),
            "columns": ",".join([ encode_column(col)
                                  for col in self.columns ])
        }


class SummingCombiner(BaseCombiner):
    """docstring for SummingCombiner"""
    classname="org.apache.accumulo.core.iterators.user.SummingCombiner"
    def __init__(self, **kw):
        super(SummingCombiner, self).__init__(**kw)

class SummingArrayCombiner(BaseCombiner):
    """docstring for SummingArrayCombiner"""
    classname = "org.apache.accumulo.core.iterators.user.SummingArrayCombiner"
    def __init__(self, **kw):
        super(SummingArrayCombiner, self).__init__(**kw)

class MaxCombiner(BaseCombiner):
    """docstring for MaxCombiner"""
    classname="org.apache.accumulo.core.iterators.user.MaxCombiner"
    def __init__(self, **kw):
        super(MaxCombiner, self).__init__(**kw)

class MinCombiner(BaseCombiner):
    """docstring for MinCombiner"""
    classname="org.apache.accumulo.core.iterators.user.MinCombiner"
    def __init__(self, **kw):
        super(MinCombiner, self).__init__(**kw)

