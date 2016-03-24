import os
import sys
import pathlib

try:
    import pyspark
except ImportError:
    _spark_home = pathlib.Path(os.environ['SPARK_HOME'])
    sys.path.extend(
        [str(p) for p in _spark_home.joinpath('python/lib').glob('*')]
    )
    import pyspark
import pyspark.sql    
    
from types import MethodType

from scape.registry import DataSource
import scape.functions
from scape.functions import tagsdim

def datasource(readerf, metadata):
    """Create a data source from a function returning a Spark DataFrame, or a DataFrame
    """
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

_sctx = pyspark.sql.SQLContext(sc)
_df = _sctx.createDataFrame([{'a':1}])
pyspark.sql.dataframe.DataFrame.add_registry = MethodType(__scape_add_registry,_df)
pyspark.sql.dataframe.DataFrame.or_filter = MethodType(__scape_or_filter, _df)
