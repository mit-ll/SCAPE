from __future__ import absolute_import

import pyspark
from functools import reduce

from scape.registry import DataSource
import scape.registry as reg

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
    md = reg._create_table_field_tagsdim_map(metadata)
    if hasattr(dataframe, '__call__'):
        return _SparkDataFrameDataSource(dataframe, md, description)
    elif isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        return _SparkDataFrameDataSource(lambda: dataframe, md, description)

_dataframe_op_dict = {
    '==': reg.Equals,
    '=~': reg.MatchesCond,
}

def _to_spark_condition(df, cond):
    if isinstance(cond, reg.Equals):
        return (df[cond.lhs.name] == cond.rhs)
    if isinstance(cond, reg.MatchesCond):
        return df[cond.lhs.name].rlike(cond.rhs)
    if isinstance(cond, reg.Or):
        parts = map(lambda c: _to_spark_condition(df, c), cond.parts)
        return reduce(lambda x,y: (x | y), parts)
    if isinstance(cond, reg.And):
        parts = map(lambda c: _to_spark_condition(df, c), cond.parts)
        return reduce(lambda x, y: (x & y), parts)

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
        if isinstance(cond, reg.TrueCondition):
            return self.select_fields(df, select)
        spark_cond = _to_spark_condition(df, cond)
        filtered = df.filter(spark_cond)
        res = self.select_fields(filtered, select)
        return res
