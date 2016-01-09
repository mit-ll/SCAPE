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
import os
import glob
from datetime import datetime, timedelta

import scape.utils
import scape.config

_log = scape.utils.new_log('scape.config.data')

def home_path(user,*parts):
    ''' Path in shared filesystem for a particular user

    E.g. given that the path in the shared fileystem for data is /gridsan

    >>> scape.config.data.home_path('myuser','splunk','raw','suricata')
    '/gridsan/myuser/splunk/raw/suricata'
    '''
    return os.path.join(scape.config.config['shared_fs_path'],user,*parts)
    

def data_path(*parts):
    ''' Path in shared filesystem for data

    E.g. given that the path in the shared fileystem for data is /gridsan/data

    >>> scape.config.data.data_path('splunk','raw','suricata')
    '/gridsan/data/splunk/raw/suricata'
    '''
    return os.path.join(scape.config.config['data_path'],*parts)

def groups_path(group):
    '''Returns path generator for a particular group directory in the
    shared filesystem

    E.g. given that the path in the shared fileystem is /gridsan

    >>> scape_group_fn = scape.config.data.groups_path('scape')
    >>> scape_group_fn('stuff')
    '/gridsan/groups/scape/splunk/raw/suricata'

    '''
    def group_path(*parts):
        return os.path.join(
            scape.config.config['shared_fs_path'],'groups',
            group,
            *parts
        )
    return group_path

def splunk_raw_data_path(index=None,dt=None):
    '''Path in shared filesystem for raw CSV splunk data.

    Args:

       index (str) [optional]: splunk index to find. If None, returns
          root raw path

       dt (datetime or string timestamp) [optional]: day for
          the directory you want to find

    >>> scape.config.data.splunk_raw_data_path()
    '/gridsan/data/splunk/raw'
    >>> scape.config.data.splunk_raw_data_path('suricata','2014-02-05')
    '/gridsan/data/splunk/raw/suricata/2014_02_05'

    '''
    parts = ['splunk','raw']

    if index is not None:
        parts.append(index)

    if dt is not None:
        dt = scape.utils.date_convert(dt)
        parts.append(dt.strftime('%Y_%m_%d'))

    return data_path(*parts)
splunk_data_path = splunk_raw_data_path

def splunk_csv_paths(index,start,end):
    '''File paths in shared filesystem for raw CSV data for a particular
    index during a particular time period.

    Args:

      index (str): splunk index

      start (datetime or string timestamp): start time of time period 

      end (datetime or string timestamp): end time of time period

    >>> scape.config.data.splunk_csv_paths('suricata','2014-03-01','2014-03-02')
    ['/data/splunk/raw/suricata/2015_03_02/suricata_20150302-000023.csv',
     ...
     '/data/splunk/raw/suricata/2015_03_02/suricata_20150302-235959.csv']
 
    '''
    import scape.utils.splunk as splunk
    paths = []
    start,end = map(scape.utils.date_convert,[start,end])
    for day_dt in scape.utils.datetime_buckets_of_size(start,end,'day'):
        root = splunk_raw_data_path(index,day_dt)
        for path in sorted(glob.glob(os.path.join(root,'*.csv'))):
            if start<=splunk.path_to_dt(path)<end:
                paths.append(path)
    return paths

def inbox_path(*parts):
    ''' Path in ingest inbox

    E.g. given an inbox path of /data/inbox

    >>> scape.config.data.inbox_path('source','test.dat')
    '/data/inbox/source/test.dat'
    '''
    return os.path.join(scape.config.config['ingest']['inbox'],*parts)
