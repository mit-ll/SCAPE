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
''' Utility functions for Accumulo Thrift API
'''

def transform_columns(columns):
    '''Transform a column spec into list of columns for scanning.

    Requested columns can be specified in two ways:

    a dict of {family:qualifier list} or

    a list of column families
    
    The output of this function will be used by the _get_scan_columns
    function below to convert into ScanColumn objects.

    '''
    
    cols = None
    if columns:
        cols = []
        if isinstance(columns, dict):
            # e.g. {"fam1": None, "fam2":["cq1","cq2"]} which means
            # pull all of column family "fam1" and only column
            # qualifiers "cq1" and "cq2" for column family "fam2"
            # 
            for fam in columns:
                if columns[fam]:
                    for q in columns[fam]:
                        cols.append([fam,q])
                else:
                    cols.append([fam])

        elif isinstance(columns,(list,tuple)):
            # .e.g. ["fam1", "fam2", ... ]
            cols.extend([[fam] for fam in columns])

    return cols

