from __future__ import absolute_import
import pandas
from types import MethodType
from scape.registry import DataSource, tagsdim
import scape.registry as reg
import functools

def datasource(readerf, metadata):
    """ Create a pandas data source 

    Args:
        readerf: Pandas DataFrame, or a function returning a Pandas DataFrame.
        metadata: :class:`scape.registry.TableMetadata` with metadata for the 
            DataFrame columns, or a dictionary in TableMetadata format.
    """
    md = reg._create_table_field_tagsdim_map(metadata)
    if hasattr(readerf,'__call__'):
        return _PandasDataFrameDataSource(readerf, md)
    elif isinstance(readerf, pandas.core.frame.DataFrame):
        return _PandasDataFrameDataSource(lambda:readerf, md)

class _MatchesCond(reg.BinaryCondition):
    pass

_pandas_op_dict = {
    '==': reg.Equals,
    '>': reg.GreaterThan,
    '>=': reg.GreaterThanEqualTo,
    '=~':  _MatchesCond
#    '<': reg.LessThan,
#    '<=': reg.LessThanEqualTo,
}

class _PandasDataFrameDataSource(DataSource):
    def __init__(self, readerf,  metadata):
        self._readerf = readerf
        super().__init__(metadata, _pandas_op_dict)

    def connect(self):
        if hasattr(self, '__dataframe'):
            return getattr(self, '__dataframe')
        """Load the associated DataFrame. """ 
        newdf = self._readerf()
        setattr(self, '__dataframe', newdf)
        setattr(newdf, '__scape_metadata', self._metadata)
        return newdf

    def _go(self, cond):
        df = self.connect()
        if isinstance(cond, reg.And):
            if len(cond.xs) == 1:
                return self._go(cond.xs[0])
            elif len(cond.xs)>1:
                return functools.reduce(lambda x,y: self._go(x) & self._go(y), cond.xs)
            else:
                raise ValueError("Empty And([])")
        elif isinstance(cond, reg.Or):
            return functools.reduce(lambda x,y: self._go(x) | self._go(x))
        elif isinstance(cond, reg.Equals):
            return df[cond.lhs.name] == cond.rhs
        elif isinstance(cond, reg.GreaterThan):
            return df[cond.lhs.name] > cond.rhs
        elif isinstance(cond, reg.GreaterThanEqualTo):
            return df[cond.lhs.name] >= cond.rhs
        elif isinstance(cond, _MatchesCond):
            return df[cond.lhs.name].str.contains(cond.rhs)
        else:
            raise ValueError("Unexpected type {}".format(str(type(cond))))

    def run(self, cond):
        df = self.connect()
        if isinstance(cond, reg.And) and not cond.xs:
            return df
        else:
            v = self._go(cond)
            return df[v]

    def check_select(self, select):
        pass

    def check_query(self, cond):
        pass
        

def __pandas_or_filter(df, dsmd, td, value):
    if isinstance(td, str):
        td = tagsdim(td)
    fields = dsmd.fields_matching(td)
    if not fields:
        print("Useless filter: Could not find fields matching: " + str(td) + " among\n" + str(dsmd))
        return df
    f,rst = fields[0],fields[1:]
    filterv = df[f.name]==value
    for f in rst:
        filterv = filterv | (df[f.name]==value)
    res = df[filterv]
    setattr(res, '__scape_metadata', df.__scape_metadata)
    return res

def __scape_add_registry(self, reg):
    setattr(self, '__scape_metadata', reg)

def __scape_or_filter(self, td, value):
    """Get all rows with a field matching the given `TagsDim`."""
    newdf = __pandas_or_filter(self, self.__scape_metadata, td, value)
    newdf.add_registry(self.__scape_metadata)
    return newdf

pandas.core.frame.DataFrame.add_registry = __scape_add_registry
pandas.core.frame.DataFrame.or_filter = __scape_or_filter
