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
import atexit
from datetime import datetime

import acqua
import acqua.build

import scape.registry.connection
import scape.utils
from scape.utils import (
    memoized_property,
)

from scape.registry.exceptions import (
    ScapeIngestError,
)
from scape.registry.acqua.utils import (
    string_list,
)

import scape.registry.acqua.count_metadata as count_metadata
import scape.registry.acqua.primary_metadata as primary_metadata
import scape.registry.acqua.table_metadata as table_metadata
import scape.registry.acqua.select_factory as select_factory
import scape.registry.acqua.ingestor as ingestor
import scape.registry.acqua.operators as operators
import scape.registry.acqua.csv_scanner as csv_scanner
import scape.registry.acqua.condition as acqua_condition


import scape.config

class AcquaDB(object):
    '''Object representation of database connection for Acqua

    Attributes:

      Connection.default_properties (class attribute): Manual encoding
         of the default.properties file associated with the Acqua
         Configuration object. If a value in default_properties is
         None, then that Configuration property is not set. If it is a
         tuple, then the first value in the tuple is used to set the
         Configuration property. If a child class of Connection sets a
         property that in the parent is a tuple to be a string, then
         that string is set to be the first element in a tuple that is
         the union of all classes' tuples.

      Connection.get_default() (classmethod): Subclasses of Connection
         can override the default_properties class attribute and the
         get_default classmethod will merge all parent classes'
         default_properties dictionaries.

      properties: Acqua Configuration properties/settings, initially
         constructed from calling the get_default() classmethod, but
         can be further modified prior to instantiating the
         Configuration object

      configuration: returns an Acqua Configuration object based on
         the Connection object's properties attribute

      database: returns an Acqua Database object based on this
         Connection's configuration

      count_metadata: returns a (wrapped) Acqua CountMetadata
         object. See the acqua.count_metadata for its access API

    >>> class MyConnection(Connection):
    ...     default_properties = {
    ...         'databaseClass': 'my.subclass.of.accumulo.database.Database',
    ...         'accumulo.user': 'user',
    ...         'accumulo.password': 'password',
    ...         'accumulo.instance': 'instance',
    ...         'accumulo.zookeepers': 'localhost',
    ...     }
    >>> conn = MyConnection()
    >>> conn.properties['databaseClass']
      ('my.subclass.of.accumulo.database.Database',
       'edu.mit.ll.acqua.database.mock.MockDatabase',
       'edu.mit.ll.acqua.database.accumulo1_5.Accumulo1_5Database')
    >>>
    >>> # get number of events associated with of source IP
    >>> # 192.168.1.1 on the day of Aug. 1st, 2014 in the "proxy"
    >>> # table
    >>> count = conn.count_metadata.count('proxy', 'src_ip',
    ...                                   '192.168.1.1', ['20140801'])
    >>>
    >>> # get dictionary of {unique source IPs: event counts} on the
    >>> # day of Aug. 1st, 2014 in the "proxy" table
    >>> unique_ips = conn.count_metadata.unique('proxy', 'src_ip',
    ...                                         ['20140801'])

    '''

    #default configuration properties
    default_properties = {
        'databaseClass': (
            'edu.mit.ll.acqua.database.mock.MockDatabase', # default
            'edu.mit.ll.acqua.database.mini.MiniClusterDatabase', 
            'edu.mit.ll.acqua.database.accumulo1_5.Accumulo1_5Database',
        ),
        'accumulo.user': None,
        'accumulo.password': None,
        'accumulo.instance': None,
        'accumulo.zookeepers': None,

        #seconds:
        'accumulo.batchwriter.timeout': 5,
        'accumulo.batchwriter.maxthreads': 2,
        #seconds:
        'accumulo.batchwriter.maxlatency': 30,
        #bytes:
        'accumulo.batchwriter.maxmemory': 52428800,
        'accumulo.batchscanner.querythreads': 8,

        'accumulo.mc.directory': None,
        # secs
        'accumulo.mc.batchwriter.timeout':5,
        # threads
        'accumulo.mc.batchwriter.maxthreads':2,
        # secs
        'accumulo.mc.batchwriter.maxlatency':30,
        # bytes
        'accumulo.mc.batchwriter.maxmemory':52428800,
        # threads
        'accumulo.mc.batchscanner.querythreads':2,

        'ingestorClass': (
            'edu.mit.ll.acqua.scapeschema.ingest.ScapeIngestor', # default
        ),
        'selectFactoryClass': (
            'edu.mit.ll.acqua.scapeschema.query.ScapeSelectFactory', # default
        ),
        'tableMetadataMapperClass': (
            'edu.mit.ll.acqua.table.TableMetadataMapper.MockTableMetadataMapper', # default
        ),

        'scapeSchema.indexSuffix': '_T',
        'scapeSchema.countSuffix': '_agg',
        'scapeSchema.writeCount': 1,
        'scapeSchema.countBlockSize': 10000,
        'scapeSchema.valueCountColumnFamily': 'count',
        'scapeSchema.columnCountColumnFamily': 'column_count',
        'scapeSchema.rowCountColumnFamily': 'row_count',
        'scapeSchema.columnFamily': 'raw',
        'scapeSchema.sourceDateFormat': 'yyyy-MM-dd HH:mm:ss',
        'scapeSchema.dateKey': 'scape_timestamp',
        'scapeSchema.dateFormat': 'yyyy-MM-dd HH:mm:ss',
        'scapeSchema.shardCount': 8,
        'scapeSchema.tableMetadataMapper': (
            'edu.mit.ll.acqua.schema.scape.DatabaseTableMetadataMapper', # default
        ),
        'scapeSchema.databaseTableMetadataMapper.tableName': 'scape_table_metadata',

        # scoreProvider options
        'scapeSchema.scoreProviderClassname': (
            'edu.mit.ll.acqua.scapeschema.query.planning.CountScoreProvider', # default
            'edu.mit.ll.acqua.scapeschema.query.planning.MockScoreProvider',
            'edu.mit.ll.acqua.scapeschema.query.catalog.Catalog',
        ),

        'scapeSchema.stringPoolSize': 1000,
        'scapeSchema.rowidName': 'scape_rowid',
        'scapeSchema.scannerTimeout': 30,
        'scapeSchema.zookeeperTimeout': 30,
        'scapeSchema.plannerThreshold': 10,
    }

    default_jvm_kw = {
        'maxheap': '512m',
        'initialheap':'128m',
        'maxheap':'512m',
        'vmargs':'-Xrs,-Djava.security.krb5.realm=,-Djava.security.krb5.kdc,-Djava.security.krb5.conf=/dev/null',
    }

    def __init__(self, log_level='INFO', **jvmkw):
        kw = self.__class__.default_jvm_kw.copy()
        kw.update(jvmkw)
        self.jvm(**kw)
        
        self.set_log_level(log_level)
        atexit.register(self.close)

    def close(self):
        if hasattr(self,'_database'):
            if self._database is not None:
                self.database.close()
                self._database = None

    def set_log_level(self,level):
        root = acqua.Logger.getRootLogger()
        root.setLevel(getattr(acqua.Level,level.upper()))

    def jvm(self,**kw):
        env = acqua.getVMEnv()
        if not env:
            classpath = acqua.build.classpath()
            acqua.initVM(
                classpath,
                **kw
            )
            env = acqua.getVMEnv()
        return env

    @classmethod
    def from_properties(cls, properties, **kw):
        cls.default_properties = properties
        return cls(**kw)

    @classmethod
    def get_default(cls):
        if object in cls.__bases__:
            return cls.default_properties.copy()
        else:
            parent = cls.__bases__[0].get_default()
            child = cls.default_properties.copy()
            for pkey in parent:
                pval = parent[pkey]
                if pkey in child:
                    cval = child[pkey]
                    if type(pval) is tuple:
                        newset = set()
                        newval = []
                        if isinstance(cval,(basestring,int)):
                            cval = (cval,)
                        for v in cval:
                            if v not in newset:
                                newval.append(v)
                                newset.add(v)
                        for v in pval:
                            if v not in newset:
                                newval.append(v)
                                newset.add(v)
                        newval = tuple(newval)
                        child[pkey] = newval
                else:
                    child[pkey] = pval
            return child

    @memoized_property
    def properties(self):
        return self.__class__.get_default()

    @memoized_property
    def configuration(self):
        config = acqua.Configuration()
        for k,v in self.properties.items():
            if type(v) is tuple:
                v = v[0]
            if v is not None:
                config.set(k,str(v))
        return config

    @memoized_property
    def database(self):
        return acqua.DatabaseFactory(self.configuration).getDatabase()

    @memoized_property
    def count_metadata(self):
        return count_metadata.CountMetadata(self)

    @memoized_property
    def primary_metadata(self):
        return primary_metadata.PrimaryMetadata(self)

    @memoized_property
    def select_factory(self):
        return select_factory.SelectFactory(self)

    def table_metadata(self, table, time_field, indexed_fields=None,
                       visibilities=None, families=None):
        '''
        Args:

          table (str): table name
          time_field (str): field where event time is located
          indexed_fields (seq): fields to index
          visibilities (dict): field visibilities
          families (dict): dictionary mapping fields to their
                                 families (if field not provided, then
                                 assume to be default)

        '''
        indexed_fields = indexed_fields or []
        meta = table_metadata.TableMetadata(
            table, time_field, indexed_fields, visibilities, families,
        )
        return meta

    def destroy_table(self, table):
        if self.database.tableExists(table):
            self.database.dropTable(table)

    def delete_rows(self, table, row_keys):
        ranges = acqua.ArrayList().of_(acqua.DatabaseScanner.Range)
        for key in row_keys:
            ranges.add(acqua.DatabaseScanner.Range(key))
        return self.database.deleteRanges(table,ranges)

    def ingestor(self, table_meta):
        '''Create an ingestor for table, keying on time_field, indexing
        indexed_fields with visibilities

        '''
        return ingestor.Ingestor(self,table_meta)

class ScapeAcquaDB(AcquaDB):
    default_properties = {
        'databaseClass': 'edu.mit.ll.acqua.database.accumulo1_5.Accumulo1_5Database',
        'accumulo.user': scape.config.config['accumulo']['user'],
        'accumulo.password': scape.config.config['accumulo']['password'],
        'accumulo.instance': scape.config.config['accumulo']['instance'],
        'accumulo.zookeepers': scape.config.config['accumulo']['zookeepers'],
        'scapeSchema.shardCount': scape.config.config['accumulo']['table']['shards']
        
    }

class AcquaConnection(scape.registry.connection.Connection):
    @memoized_property
    def acqua(self):
        raise NotImplementedError('Must subclass AcquaConnection')

    # def connect(self):
    #     self.acqua
        
    def destroy_tables(self,tabular):
        for table in tabular['table']:
            self.acqua.destroy_table(table)
            self.acqua.destroy_table(table+'_T')
            self.acqua.destroy_table(table+'_agg')
    
    def _table_metadata(self, tabular):
        table_metas = []
        for T in tabular:
            table_name = T.node['table']
            time_field = T.node['time']
            indexed_fields = [f.node['name'] for f in T.fields if f.dims]
            visibilities = {}
            for f in T.fields:
                if 'vis' in f.node:
                    famqual = '{}:{}'.format(f.node['family'],f.node['name'])
                    visibilities[famqual] = f.node['vis']
            families = {f.node['name']:f.node['family'] for f in T.fields}
            meta = self.acqua.table_metadata(
                table_name, time_field, indexed_fields=indexed_fields,
                visibilities=visibilities,families=families,
            )
            table_metas.append(meta)
        return table_metas

    def last_times(self, tabular):
        metas = self._table_metadata(tabular)
        times = filter(None,[self.acqua.primary_metadata.last_time(m)
                             for m in metas])
        return times

    def set_log_level(self,level):
        super(AcquaConnection,self).setup_logging(level)
        self.acqua.set_log_level(level)

    def ingestor(self, tabular):
        if len(tabular) < 1:
            raise ScapeIngestError('Cannot ingest into empty Selection')
        elif len(tabular) > 1:
            raise ScapeIngestError(
                'Ambiguous Selection: resolves to ({}) tabular'
                ' elements... which one are you ingesting'
                ' into?'.format(', '.join(tabular.names))
            )
            
        meta = self._table_metadata(tabular)[0]

        return self.acqua.ingestor(meta)
        
    def create_tables(self, tabular):
        for T in tabular:
            with self.ingestor(T) as ingestor:
                ingestor.create_table()

    def ingest_csv(self, tabular, *paths):
        CSV = csv_scanner.CSVScanner(paths)
        with self.ingestor(tabular) as ingestor:
            ingestor.ingest_operator(CSV.operator)
        
    def ingest_json(self, tabular, *paths):
        raise NotImplementedError
        
    def ingest_xml(self, tabular, *paths):
        raise NotImplementedError
        
    def ingest_rows(self, tabular, row_iterator_or_sequence):
        S = operators.PythonSource(row_iterator_or_sequence)
        with self.ingestor(tabular) as ingestor:
            ingestor.ingest_operator(S.operator)

    @staticmethod
    def _ts_dt(ts):
        tsmap = {
            4: '%Y',
            6: '%Y%m',
            8: '%Y%m%d',
            10: '%Y%m%d%H',
            12: '%Y%m%d%H%M',
            14: '%Y%m%d%H%M%S',
        }
        return datetime.strptime(ts,tsmap[len(ts)])

    def row_counts(self, tabular, start, end, granularity):
        buckets = scape.utils.timestamp_buckets_of_size(start,end,granularity)
        lut = {}
        for T in tabular:
            table = T.node['table']
            counts = self.acqua.count_metadata.row_counts(table,buckets)
            for ts,cnt in counts:
                if cnt:
                    dt = self._ts_dt(ts)
                    val = lut.setdefault(dt,0)
                    lut[dt] = val + cnt
        return lut

    def dim_counts(self, tabular, start, end, granularity, tagged_dims):
        buckets = scape.utils.timestamp_buckets_of_size(start,end,granularity)
        lut = {}
        for T in tabular:
            table = T.node['table']
            for tdim in tagged_dims:
                for field in T.fields.has_any(tdim).names:
                    tdlut = lut.setdefault(tdim,{})
                    counts = self.acqua.count_metadata.column_counts(
                        table,field,buckets,
                    )
                    for ts,cnt in counts:
                        if cnt:
                            dt = self._ts_dt(ts)
                            val = tdlut.setdefault(dt,0)
                            tdlut[dt] = val + cnt
        return lut

    def value_counts(self, conditions, start, end, granularity):
        buckets = scape.utils.timestamp_buckets_of_size(start,end,granularity)
        lut = {}
        for T,condition in conditions.items():
            table = T.node['table']
            for eq in condition.equals_conditions:
                field = eq.field
                tdim = eq.tagged_dim if eq.tagged_dim else field
                value = eq.value
                tdlut = lut.setdefault(tdim,{}).setdefault(value,{})
                counts = self.acqua.count_metadata.value_counts(
                    table,field,value,buckets,
                )
                for ts,cnt in counts:
                    if cnt:
                        dt = self._ts_dt(ts)
                        val = tdlut.setdefault(dt,0)
                        tdlut[dt] = val + cnt
        return lut

    def unique(self, tabular, start, end, tagged_dims):
        buckets = scape.utils.timestamp_buckets_of_max_size(start,end)
        count_lut = {}
        for td in tagged_dims:
            for T in tabular:
                table = T.node['table']
                for field in T.fields.has_any(td).names:
                    counts = self.acqua.count_metadata.unique(
                        table,field,buckets,
                    )
                    lut = count_lut.setdefault(td.d,{})
                    for value,count in counts.items():
                        c = lut.setdefault(value,0)
                        lut[value] = c+count
        return count_lut
        
    def selects(self, start, end, conditions):
        selects = {}
        for T, condition in conditions.items():
            table = T.node['table']
            where = acqua_condition.resolve_where(condition)
            selects[T] = self.acqua.select_factory.select(
                table, start, end, where,
            )
        return selects
        
class MockAcquaConnection(AcquaConnection):
    @memoized_property
    def acqua(self):
        return AcquaDB(**self._db_connection_kw)


class ScapeAcquaConnection(AcquaConnection):
    @memoized_property
    def acqua(self):
        return ScapeAcquaDB(**self._db_connection_kw)
        
