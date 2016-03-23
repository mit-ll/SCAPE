from __future__ import absolute_import

import pyspark
from types import MethodType

from scape.registry import DataSource
import scape.functions
from scape.functions import tagsdim

def datasource(readerf, metadata):
    """Create a data source from a Spark DataFrame or a function returning a DataFrame"""
    md = scape.functions._create_table_field_tagsdim_map(metadata)
    if hasattr(readerf, '__call__'):
        return _SparkDataFrameDataSource(readerf, md)
    elif isinstance(readerf, pyspark.sql.dataframe.DataFrame):
        return _SparkDataFrameDataSource(lambda: readerf, md)

class _SparkDataFrameDataSource(DataSource):
    def __init__(self, readerf, registry):
        self._readerf = readerf
        self._registry = registry
    
    def connect(self):
        newdf = self._readerf()
        setattr(newdf, '__scape_metadata', self._registry)
        return newdf

def __or_filtered(df, dsmd, td, value):
    if isinstance(td, str):
        td = tagsdim(td)
    fields = dsmd.fields_matching(td)
    if not fields:
        print("Useless filter: Could not find fields matching: " + str(td) + " among\n" + str(dsmd))
        return df
    f,rst = fields[0],fields[1:]
    filterv = df[f]==value
    for f in rst:
        filterv = filterv | (df[f]==value)
    return df.filter(filterv)

def __scape_add_registry(self, reg):    
    self.__scape_metadata = reg

def __scape_or_filter(self, td, value):
    newdf = __or_filtered(self, self.__scape_metadata, td, value)
    newdf.__scape_metadata = self.__scape_metadata
    return newdf
    
pyspark.sql.dataframe.DataFrame.add_registry = __scape_add_registry
pyspark.sql.dataframe.DataFrame.or_filter = __scape_or_filter
