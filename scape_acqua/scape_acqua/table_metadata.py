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

'''Wrapper for Acqua TableMetadata, a persistant mapping of column
names to index types

'''
import acqua

class TableMetadata(object):
    ''' Acqua TableMetadata wrapper object

    >>> tm = TableMetadata('proxy', 'timecol',
    ...                    [('req_domain','value'),'src_ip','dest_ip'])
    >>>
    '''
    type_map = {
        'none': 'NO_INDEX',
        'time': 'TIME_INDEX',
        'value': 'VALUE_INDEX',
        'fulltext': 'FULL_TEXT_INDEX',
        'geolocation': 'GEOLOCATION_INDEX',
    }

    def __init__(self, table, time_field, indexed_fields,
                 visibilities=None, families=None):
        '''TableMetadata wrapper

        Args:
          table (str): table name

          time_field (str): field of table where event time is stored

          indexed_fields (seq): sequence of either indexed fields
             (str) or (indexed field (str), index type (str))
             tuples

          visibilities (dict): dictionary of
              {"field_family:field_name": <visibility>} mappings

          families (dict): dictionary of {"field":"family"}
              mappings. If a field is not provided, then assumed to
              have default family

        If a string is provided in indexed_fields, then it is assumed
        to be of index type VALUE_INDEX. If a tuple is provided, then
        the first element of the tuple is the field to be indexed and
        the second is the index type as a string.

        If visibilities is provided and if a dictionary key does
        not contain a colon, then the key will have a colon prepended
        to it (i.e. a null column family will be provided)

        **WARNING** This object assumes the JVM has already been
          initialized. If it hasn't, then dereferencing the index type
          constants of Acqua's TableMetadata class will result in an
          AttributeError

        '''
        self.table = table
        self.time_field = time_field
        self.indexed_fields = indexed_fields
        self.visibilities = visibilities
        self.families = families

    @property
    def metadata(self):
        fields = acqua.HashMap().of_(acqua.String, acqua.Integer)
        try:
            type_map = {k: getattr(acqua.TableMetadata, self.type_map[k])
                        for k in self.type_map}
        except AttributeError:
            raise AttributeError(
                "Cannot access static constants in acqua.TableMetadata."
                " Did you forget to initialize the JVM?"
            )

        for obj in self.indexed_fields:
            if isinstance(obj, basestring):
                column = obj
                index_type = type_map['value']
            else:
                column, index_type = obj
                index_type = type_map[index_type]
            fields.put(column, index_type)

        vis = acqua.HashMap().of_(acqua.String,acqua.String)
        if self.visibilities:
            for famQual,vstr in self.visibilities.items():
                if ':' not in famQual:
                    famQual = ":"+famQual
                vis.put(famQual,vstr)

        fam = acqua.HashMap().of_(acqua.String,acqua.String)
        if self.families:
            for field,family in self.families.items():
                fam.put(field,family)

        return acqua.TableMetadata(self.table, self.time_field, fields,
                                   vis, fam)

