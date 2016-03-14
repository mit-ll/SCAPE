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

from scape.utils.decorators import (
    memoized_property
)

import scape.config

from scape.registry.registry import Registry

class Connection(object):
    def __init__(self, paths=None, dicts=None, registry_dict=None,
                 **db_connection_kw):
        self._paths = paths
        self._dicts = dicts
        self._registry_dict = registry_dict
        self._db_connection_kw = db_connection_kw.copy()

    def __getitem__(self,key):
        return self._db_connection_kw[key]

    @property
    def paths(self):
        return self._paths or scape.config.config['registry']['paths']
    @property
    def dicts(self):
        return self._dicts        
    @property
    def registry_dict(self):
        return self._registry_dict        
            
    @memoized_property
    def registry(self):
        paths,dicts,registry_dict = self.paths,self.dicts,self.registry_dict
        return Registry(
            paths=paths, dicts=dicts, registry_dict=registry_dict,
            connection=self,
        )

    def pickle(self):
        ''' Pickle a connection 
        '''
        return (self.__class__.__module__, self.__class__.__name__,
                self.paths, self.dicts,
                self.registry_dict, self._db_connection_kw,)

    @classmethod
    def unpickle(cls, module, classname, paths, dicts,
                 registry_dict, db_connection_kw):
        mod = __import__(module,fromlist=[classname])
        klass = getattr(mod,classname)
        return klass(paths,dicts,registry_dict,**db_connection_kw)

    def event_op(self,function):
        import scape.registry.question
        args = self.pickle()
        def wrapper(row):
            event = scape.registry.question.Event.unpickle(row,args)
            # return function(event)
            retval = function(event)
            if isinstance(retval,scape.registry.question.Event):
                retval = retval.pickle()
            return retval
        return wrapper

    def ingest_op(self,function):
        args = self.pickle()
        import scape.registry.connection
        def wrapper(*a):
            C = scape.registry.connection.Connection.unpickle(*args)
            retval = function(C,*a)
            # if isinstance(retval,scape.registry.question.Event):
            #     retval = retval.pickle()
            return retval
        return wrapper

    def set_log_level(self,level):
        scape.config.setup_logging(level=level)

    def connect(self):
        raise NotImplementedError('Must implement in a subclass of Connection')

    def create_tables(self, tabular):
        raise NotImplementedError('Must implement in a subclass of Connection')

    def destroy_tables(self, tabular):
        raise NotImplementedError('Must implement in a subclass of Connection')

    def last_times(self, tabular):
        raise NotImplementedError('Must implement in a subclass of Connection')

    def ingest_csv(self, tabular, *paths):
        raise NotImplementedError('Must implement in a subclass of Connection')
        
    def ingest_json(self, tabular, *paths):
        raise NotImplementedError('Must implement in a subclass of Connection')
        
    def ingest_xml(self, tabular, *paths):
        raise NotImplementedError('Must implement in a subclass of Connection')
        
    def ingest_rows(self, tabular, row_iterator_or_sequence):
        raise NotImplementedError('Must implement in a subclass of Connection')
        
    def row_counts(self, tabular, start, end, granularity):
        raise NotImplementedError('Must implement in a subclass of Connection')

    def dim_counts(self, tabular, start, end, granularity, tagged_diminsions):
        raise NotImplementedError('Must implement in a subclass of Connection')

    def value_counts(self, conditions, start, end, granularity):
        raise NotImplementedError('Must implement in a subclass of Connection')

    def unique(self, tabular, start, end, tagged_dims):
        raise NotImplementedError('Must implement in a subclass of Connection')

    def selects(self, start, end, conditions):
        raise NotImplementedError('Must implement in a subclass of Connection')
        
