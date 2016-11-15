from __future__ import absolute_import

import pyspark
from functools import reduce

from scape.registry import DataSource
import scape.registry as _reg
from scape.registry.table_metadata import create_table_field_tagged_dim_map as _create_metadata

def datasource(dataframe, metadata, description=""):
    '''Create a data source from a Spark DataFrame or a function returning a DataFrame

    Args:
      reader: A dataframe of a function returning a dataframe
      metadata: Mapping from field names to tags and dimensions
      description: An optional description

    Returns:
      The Scape datasource wrapping the dataframe supporting equality and regex conditions
      using '==' and '=~'
    '''
    md = _create_metadata(metadata)
    if hasattr(dataframe, '__call__'):
        return _SparkDataFrameDataSource(dataframe, md, description)
    elif isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        return _SparkDataFrameDataSource(lambda: dataframe, md, description)

_dataframe_op_dict = {
    '==': _reg.Equals,
    '=~': _reg.MatchesCond,
    '<' : _reg.LessThan,
    '<=': _reg.LessThanEqualTo,
    '>' : _reg.GreaterThan,
    '>=': _reg.GreaterThanEqualTo
}

def _to_spark_condition(df, cond):
    if isinstance(cond, _reg.Equals):
        return (df[cond.lhs.name] == cond.rhs)
    elif isinstance(cond, _reg.MatchesCond):
        return df[cond.lhs.name].rlike(cond.rhs)
    elif isinstance(cond, _reg.LessThan):
        return df[cond.lhs.name] < cond.rhs
    elif isinstance(cond, _reg.LessThanEqualTo):
        return df[cond.lhs.name] <= cond.rhs
    elif isinstance(cond, _reg.GreaterThan):
        return df[cond.lhs.name] > cond.rhs
    elif isinstance(cond, _reg.GreaterThanEqualTo):
        return df[cond.lhs.name] >= cond.rhs
    elif isinstance(cond, _reg.Or):
        parts = map(lambda c: _to_spark_condition(df, c), cond.parts)
        return reduce(lambda x,y: (x | y), parts)
    elif isinstance(cond, _reg.And):
        parts = map(lambda c: _to_spark_condition(df, c), cond.parts)
        return reduce(lambda x, y: (x & y), parts)
    else:
        raise ValueError("Unknown condition: " + str(cond))


class _SparkDataFrameDataSource(DataSource):
    def __init__(self, readerf, metadata, description):
        super(_SparkDataFrameDataSource, self).__init__(metadata, description, _dataframe_op_dict)
        self._readerf = readerf
        self._df = None
        self._metadata = metadata

    def connect(self):
        if (self._df):
            return self._df
        self._df = self._readerf()
        return self._df

    def select_fields(self, df, select):
        if select.fields:
            return df.select(self._field_names(select))
        else:
            return df

    def run(self, select):
        ''' Return a dataframe with the given selection.
        '''
        cond = self._rewrite(select.condition)
        df = self.connect()
        if isinstance(cond, _reg.TrueCondition):
            return self.select_fields(df, select)
        spark_cond = _to_spark_condition(df, cond)
        filtered = df.filter(spark_cond)
        res = self.select_fields(filtered, select)
        return res

