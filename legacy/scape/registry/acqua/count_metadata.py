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
import acqua
import datetime

def timestamp_array(times):
    times_array = acqua.ArrayList().of_(acqua.String)
    for t in times:
        if isinstance(t,basestring):
            pass
        elif isinstance(t,datetime.datetime):
            t = t.strftime('%Y%m%d%H%M')
        times_array.add(t)
    return times_array
    
class CountMetadata(object):
    ''' Wrapper object for Acqua CountMetadata
    '''
    def __init__(self,connection):
        self.metadata = acqua.CountMetadata(
            connection.configuration, connection.database
        )

    def unique(self, table, field, times):
        '''Given table, field and sequence of times, return dictionary of
        unique values and their corresponding counts

        Args:
          table (str): database table name
          field (str): field in table
          times (seq): sequence of timestamps (or datetime objects)

        Returns:
          dictionary of {value(str): count(int),...}

        The values in the times must be sent to the CountMetadata
        object as strings. The format of these strings encodes the
        time period under consideration::

          YYYYmmddHHMM -- for one minute 
          YYYYmmddHH -- for one hour
          YYYYmmdd -- for one day
          YYYYmm -- for one month
          YYYY -- for one year

        If times contains a datetime, then it is assumed to reference
        a one minute period.

        '''
        times_array = timestamp_array(times)
        
        unique_map = self.metadata.getUnique(table, field, times_array)
        if unique_map:
            return {k:unique_map.get(k) for k in unique_map.keySet()}
        return {}

    def count(self, table, field, value, times):
        '''Given table, field, value and sequence of times, return event count
        for that value during those times

        Args:
          table (str): database table name
          field (str): field in table
          value (str): value 
          times (seq): sequence of timestamps (or datetime objects)

        Returns:
          integer number of events

        The values in the times must be sent to the CountMetadata
        object as strings. The format of these strings encodes the
        time period under consideration::

          YYYYmmddHHMM -- for one minute 
          YYYYmmddHH -- for one hour
          YYYYmmdd -- for one day
          YYYYmm -- for one month
          YYYY -- for one year

        If times contains a datetime, then it is assumed to reference
        a one minute period.

        '''
        times_array = timestamp_array(times)

        count = self.metadata.getCount(table, field, value, times_array)
        if count is not None:
            count = count.intValue()
        return count

    def value_counts(self, table, field, value, times):
        '''Given table, field, value and sequence of times, return event count
        for that value during those times

        Args:
          table (str): database table name
          field (str): field in table
          value (str): value 
          times (seq): sequence of timestamps (or datetime objects)

        Returns:
          sequence of (timestamp, count) tuples

        The values in the times must be sent to the CountMetadata
        object as strings. The format of these strings encodes the
        time period under consideration::

          YYYYmmddHHMM -- for one minute 
          YYYYmmddHH -- for one hour
          YYYYmmdd -- for one day
          YYYYmm -- for one month
          YYYY -- for one year

        If times contains a datetime, then it is assumed to reference
        a one minute period.

        '''
        times_array = timestamp_array(times)

        counts = self.metadata.getValueCounts(table, field, value, times_array)
        if counts is not None:
            counts = [(ts,counts.get(ts)) for ts in counts.keySet()]
        return counts

    def column_counts(self, table, field, times):
        '''Given table, field and sequence of times, return event count
        for that field during those times

        Args:
          table (str): database table name
          field (str): field in table
          value (str): value 
          times (seq): sequence of timestamps (or datetime objects)

        Returns:
          sequence of (timestamp, count) tuples

        The values in the times must be sent to the CountMetadata
        object as strings. The format of these strings encodes the
        time period under consideration::

          YYYYmmddHHMM -- for one minute 
          YYYYmmddHH -- for one hour
          YYYYmmdd -- for one day
          YYYYmm -- for one month
          YYYY -- for one year

        If times contains a datetime, then it is assumed to reference
        a one minute period.

        '''
        times_array = timestamp_array(times)

        counts = self.metadata.getColumnCounts(table, field, times_array)
        if counts is not None:
            counts = [(ts,counts.get(ts)) for ts in counts.keySet()]
        return counts

    def row_counts(self, table, times):
        '''Given table and sequence of times, return event count for that
        value during those times

        Args:
          table (str): database table name
          times (seq): sequence of timestamps (or datetime objects)

        Returns:
          sequence of (timestamp, count) tuples

        The values in the times must be sent to the CountMetadata
        object as strings. The format of these strings encodes the
        time period under consideration::

          YYYYmmddHHMM -- for one minute 
          YYYYmmddHH -- for one hour
          YYYYmmdd -- for one day
          YYYYmm -- for one month
          YYYY -- for one year

        If times contains a datetime, then it is assumed to reference
        a one minute period.

        '''
        times_array = timestamp_array(times)

        counts = self.metadata.getRowCounts(table, times_array)
        if counts is not None:
            counts = [(ts,counts.get(ts)) for ts in counts.keySet()]
        return counts


