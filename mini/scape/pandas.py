from __future__ import absolute_import
import pandas
from types import MethodType
from scape.registry import DataSource
import scape.functions
from scape.functions import tagsdim

def datasource(readerf, metadata):
    md = scape.functions._create_table_field_tagsdim_map(metadata)
    if hasattr(readerf,'__call__'):
        return _PandasDataFrameDataSource(readerf, md)
    elif isinstance(readerf, pandas.core.frame.DataFrame):
        return _PandasDataFrameDataSource(lambda:readerf, md)

class _PandasDataFrameDataSource(DataSource):
    def __init__(self, readerf,  metadata):
        self._readerf = readerf
        self._metadata = metadata

    def connect(self):
        newdf = self._readerf()
        setattr(newdf, '__scape_metadata', self._metadata)
        return newdf

def __pandas_or_filter(df, dsmd, td, value):
    if isinstance(td, basestring):
        td = tagsdim(td)
    fields = dsmd.fields_matching(td)
    if not fields:
        print "Useless filter: Could not find fields matching: " + str(td) + " among\n" + str(dsmd)
        return df
    f,rst = fields[0],fields[1:]
    filterv = df[f]==value
    for f in rst:
        filterv = filterv | (df[f]==value)
    res = df[filterv]
    setattr(res, '__scape_metadata', df.__scape_metadata)
    return res

def __scape_add_registry(self, reg):
    setattr(self, '__scape_metadata', reg)
def __scape_or_filter(self, td, value):
    newdf = __pandas_or_filter(self, self.__scape_metadata, td, value)
    newdf.add_registry(self.__scape_metadata)
    return newdf

pandas.core.frame.DataFrame.add_registry = MethodType(__scape_add_registry, None, pandas.core.frame.DataFrame)
pandas.core.frame.DataFrame.or_filter = MethodType(__scape_or_filter, None, pandas.core.frame.DataFrame)
