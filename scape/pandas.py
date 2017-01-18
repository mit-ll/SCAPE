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

from __future__ import absolute_import
import pandas
from types import MethodType
from scape.registry import DataSource
#from scape.registry.tagged_dim import TaggedDim, tagged_dim
from scape.registry.table_metadata import create_table_field_tagged_dim_map
import scape.registry as reg
import functools

def datasource(readerf, metadata, description=None):
    """ Create a pandas data source 

    Args:
        readerf: Pandas DataFrame, or a function returning a Pandas DataFrame.
        metadata: :class:`scape.registry.TableMetadata` with metadata for the 
            DataFrame columns, or a dictionary in TableMetadata format.
    """
    md = create_table_field_tagged_dim_map(metadata)
    if hasattr(readerf,'__call__'):
        return _PandasDataFrameDataSource(readerf, md, description)
    elif isinstance(readerf, pandas.core.frame.DataFrame):
        return _PandasDataFrameDataSource(lambda:readerf, md, description)

class _MatchesCond(reg.BinaryCondition):
    pass

_pandas_op_dict = {
    '==': reg.Equals,
    '!=': reg.NotEqual,
    '>': reg.GreaterThan,
    '>=': reg.GreaterThanEqualTo,
    '=~':  _MatchesCond,
   '<': reg.LessThan,
   '<=': reg.LessThanEqualTo
}

class _PandasDataFrameDataSource(DataSource):
    def __init__(self, readerf,  metadata, description):
        self._readerf = readerf
        desc = description if description else "Pandas DataSource"
        super(_PandasDataFrameDataSource, self).__init__(metadata, desc, _pandas_op_dict)

    def connect(self):
        if hasattr(self, '__dataframe'):
            return getattr(self, '__dataframe')
        """Load the associated DataFrame. """ 
        newdf = self._readerf()
        setattr(self, '__dataframe', newdf)
        return newdf

    def _go(self, cond):
        df = self.connect()
        if isinstance(cond, reg.And):
            if len(cond._parts) == 1:
                return self._go(cond._parts[0])
            elif len(cond._parts)>1:
                return functools.reduce(lambda x,y: self._go(x) & self._go(y), cond._parts)
            else:
                raise ValueError("Empty And([])")
        elif isinstance(cond, reg.Or):
            xs = cond._parts
            if len(xs)==0:
                raise ValueError("Empty Or([])")
            elif len(xs)==1:
                return self._go(xs[0])
            elif len(xs)>1:
                return functools.reduce(lambda x,y: self._go(x) | self._go(y), xs)
        elif isinstance(cond, reg.Equals):
            return df[cond.lhs.name] == cond.rhs
        elif isinstance(cond, reg.NotEqual):
            return df[cond.lhs.name] != cond.rhs
        elif isinstance(cond, reg.GreaterThan):
            return df[cond.lhs.name] > cond.rhs
        elif isinstance(cond, reg.GreaterThanEqualTo):
            return df[cond.lhs.name] >= cond.rhs
        elif isinstance(cond, reg.LessThan):
            return df[cond.lhs.name] < cond.rhs
        elif isinstance(cond, reg.LessThanEqualTo):
            return df[cond.lhs.name] <= cond.rhs
        elif isinstance(cond, _MatchesCond):
            return df[cond.lhs.name].str.contains(cond.rhs)
        else:
            raise ValueError("Unexpected type {}".format(str(type(cond))))

    def run(self, select):
        cond = self._rewrite(select.condition)
        df = self.connect()
        if isinstance(cond, reg.And) and not cond._parts:
            return df
        else:
            v = self._go(cond)
            return df[v]

    def check_select(self, select):
        pass

    def check_query(self, cond):
        pass
