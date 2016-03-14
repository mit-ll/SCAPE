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

''' Data access utilities for Scape based on configuration
'''

import os
import glob
from datetime import datetime, timedelta

import scape.utils

from .errors import (
    ScapeConfigError, ScapeConfigDataError, 
)
from .config import (
    ConfigBase
)

_log = scape.utils.new_log('scape.config.data')

class ConfigData(ConfigBase):
    '''Scape configuration data/filesystem helper mixin.

    The assumption of this mixin class is that there exists:

    1) A directory in a shared file system (e.g. NFS or Lustre FS)
       where data is available for common use and

    2) A local data directory for operations that require local
       storage (e.g. sqlite3 dbs that require file locking, something
       the Lustre file systems can't provide).

    This class then provides helper methods that provide information
    about the relevant path structure of these file systems.

    '''

    def home_path(self, user, *parts):
        '''Path in shared filesystem for a particular user

        E.g. given that the path for users in the shared fileystem for
        data is /nfs/home

        >>> config = scape.config.default_config()
        >>> config.home_path('myuser','splunk','raw','suricata')
        '/nfs/home/myuser/splunk/raw/suricata'

        '''
        return os.path.join(self['data']['shared_fs_path'],user,*parts)

    def shared_data_path(self, *parts):
        '''Path in shared filesystem for data

        E.g. given that the path in the shared fileystem for data is
        /nfs/data

        >>> config = scape.config.default_config()
        >>> config.shared_data_path('splunk','raw','suricata')
        '/nfs/data/splunk/raw/suricata'

        '''
        return os.path.join(self['data']['shared_data_path'],*parts)

    def groups_path(self, group):
        '''Returns path generator for a particular group directory in the
        shared filesystem

        E.g. given that the path in the shared fileystem is /nfs

        >>> config = scape.config.default_config()
        >>> scape_group_fn = config.groups_path('scape')
        >>> scape_group_fn('stuff')
        '/nfs/groups/scape/splunk/raw/suricata'

        '''
        def group_path(*parts):
            return os.path.join(
                self['data']['shared_fs_path'],'groups',
                group,
                *parts
            )
        return group_path

    def splunk_raw_data_path(self, index=None, dt=None):
        '''Path in shared filesystem for raw CSV splunk data.

        Parameters:

        - index (str) [optional]: splunk index to find. If None,
          returns root raw path

        - dt (datetime or string timestamp) [optional]: day for the
          directory you want to find

        >>> config = scape.config.default_config()
        >>> config.splunk_raw_data_path()
        '/nfs/data/splunk/raw'
        >>> config.splunk_raw_data_path('suricata','2014-02-05')
        '/nfs/data/splunk/raw/suricata/2014_02_05'

        '''
        parts = ['splunk','raw']

        if index is not None:
            parts.append(index)

        if dt is not None:
            dt = scape.utils.date_convert(dt)
            parts.append(dt.strftime('%Y_%m_%d'))

        return self.data_path(*parts)

    def splunk_csv_paths(self, index, start, end):
        '''File paths in shared filesystem for raw CSV data for a particular
        index during a particular time period.

        Parameters:

        - index (str): splunk index
        
        - start (datetime or string timestamp): start time of time
          period

        - end (datetime or string timestamp): end time of time period

        >>> config = scape.config.default_config()
        >>> config.splunk_csv_paths('suricata','2014-03-01','2014-03-02')
        ['/data/splunk/raw/suricata/2015_03_02/suricata_20150302-000023.csv',
         ...
         '/data/splunk/raw/suricata/2015_03_02/suricata_20150302-235959.csv']

        '''
        import scape.utils.splunk as splunk
        paths = []
        start,end = map(scape.utils.date_convert,[start,end])
        for day_dt in scape.utils.datetime_buckets_of_size(start,end,'day'):
            root = self.splunk_raw_data_path(index,day_dt)
            for path in sorted(glob.glob(os.path.join(root,'*.csv'))):
                if start<=splunk.path_to_dt(path)<end:
                    paths.append(path)
        return paths

    def inbox_path(self, *parts):
        ''' Path in ingest inbox

        E.g. given an inbox path of /data/inbox

        >>> config = scape.config.default_config()
        >>> config.inbox_path('source','test.dat')
        '/data/inbox/source/test.dat'
        '''
        return os.path.join(self['data']['ingest']['inbox'],*parts)

def _default_config():
    import scape.config
    return scape.config.default_config()

def home_path(user,*parts):
    return _default_config().home_path(user,*parts)
home_path.__doc__ = ConfigData.home_path.__doc__
    
def data_path(*parts):
    return _default_config().shared_data_path(*parts)
data_path.__doc__ = ConfigData.shared_data_path.__doc__

def groups_path(group):
    return _default_config().groups_path(group)
groups_path.__doc__ = ConfigData.groups_path.__doc__

def splunk_raw_data_path(index=None,dt=None):
    return _default_config().splunk_raw_data_path(index,dt)
splunk_raw_data_path.__doc__ = ConfigData.splunk_raw_data_path.__doc__

splunk_data_path = splunk_raw_data_path

def splunk_csv_paths(index,start,end):
    return _default_config().splunk_csv_paths(index,start,end)
splunk_csv_paths.__doc__ = ConfigData.splunk_csv_paths.__doc__

def inbox_path(*parts):
    return _default_config().inbox_path(*parts)
inbox_path.__doc__ = ConfigData.inbox_path.__doc__
