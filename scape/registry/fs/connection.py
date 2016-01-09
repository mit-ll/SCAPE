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
import csv
import json
import re
from datetime import datetime

import scape.registry.connection
import scape.utils
from scape.utils import (
    memoized_property,
)

from scape.registry.exceptions import (
    ScapeIngestError,
)
from scape.registry.event import (
    Event,
)

import scape.config

class FileConnection(scape.registry.connection.Connection):

    def _table_file_name(self,T):
        return T['table'][0]+'.csv'

    _tabular_event_cache = None
    def events(self,T):
        if self._tabular_event_cache is None:
            self._tabular_event_cache = {}
        events = self._tabular_event_cache.setdefault(T,None)
        if events is None:
            file_name = self._table_file_name(T)
            events = []
            if os.path.exists(file_name):
                events = [Event(row,T)
                          for row in list(scape.utils.csv_rows(file_name))]
                self._tabular_event_cache[T] = events
            self._tabular_event_cache[T] = events
        return events

    def sync(self):
        for T, events in self._tabular_event_cache.items():
            columns = set()
            rows = [e.row for e in events]
            for r in rows:
                columns.update(r.keys())
            columns = sorted(columns)
            with open(self._table_file_name(T),'wb') as wfp:
                writer = csv.DictWriter(wfp,columns)
                writer.writeheader()
                writer.writerows(rows)

    def destroy_tables(self,tabular):
        for table in tabular['table']:
            os.unlink(table+'.csv')
    
    def last_times(self, tabular):
        all_dts = [datetime(1,1,1)]
        for T in tabular:
            dts = [event.datetime for event in self.events(T)]
            if dts:
                all_dts.append(max(dts))
        return all_dts

    def create_tables(self, tabular):
        for T in tabular:
            file_name = self._table_file_name(T)
            with open(file_name,'a') as wfp:
                pass

    def ingest_csv(self, tabular, *paths):
        def rowgen():
            for path in paths:
                for row in scape.utils.csv_rows(path):
                    yield row
        self.ingest_rows(tabular, rowgen())
            
    def ingest_json(self, tabular, *paths):
        def rowgen():
            for path in paths:
                new_rows = []
                with open(path) as rfp:
                    for line in rfp:
                        yield json.loads(line)
        self.ingest_rows(tabular, rowgen())
        
    def ingest_xml(self, tabular, *paths):
        raise NotImplementedError
        
    def ingest_rows(self, tabular, row_iterator_or_sequence):
        if len(tabular) < 1:
            raise ScapeIngestError('Cannot ingest into empty Selection')
        elif len(tabular) > 1:
            raise ScapeIngestError(
                'Ambiguous Selection: resolves to ({}) tabular'
                ' elements... which one are you ingesting'
                ' into?'.format(', '.join(tabular.names))
            )
        for T in tabular:
            events = self.events(T)
            for row in row_iterator_or_sequence:
                events.append(Event(row,T))
        self.sync()

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
        return {}

    def dim_counts(self, tabular, start, end, granularity, tagged_dims):
        return {}

    def value_counts(self, conditions, start, end, granularity):
        return {}

    def unique(self, tabular, start, end, tagged_dims):
        return {}
        
    def selects(self, start, end, conditions):
        def binary_boolean(eval_op, node):
            eval_atoms = [
                "({})" for child in node.children
            ]
            for i,child in enumerate(node.children):
                child_eval_string = condition_to_eval_string(child)
                eval_atoms[i] = eval_atoms[i].format(
                    child_eval_string,
                )
            return eval_op.join(eval_atoms)
            
        def condition_to_eval_string(node):
            if node.type in {'and','or'}:
                eval_op = ' {} '.format(node.type)
                return binary_boolean(eval_op, node)

            elif node.type == 'not':
                return 'not ({})'.format(binary_boolean(' and ',node))

            elif node.type == 'equals':
                return 'row.get("{field}") == "{value}"'.format(
                    field = node.field,
                    value = node.value,
                )
            elif node.type == 'regex':
                return 're.search(r"{pattern}",row.get("{field}",""))'.format(
                    field = node.field,
                    pattern = node.value,
                )
            
        def in_window(event):
            return start <= event.datetime < end

        eval_string_map = {}

        def meets_condition(event, condition):
            if condition is None:
                return True
            row = event.row
            T = event.selection
            eval_string = eval_string_map.get(T)
            if eval_string is None:
                eval_string = condition_to_eval_string(condition)
                print eval_string
                eval_string_map[T] = eval_string
            return eval(eval_string)
            
        def select(T,condition):
            for event in self.events(T):
                if (in_window(event) and
                    meets_condition(event,condition)):
                    yield event.row
                    
        selects = {}
        for T, condition in conditions.items():
            print 'condition:',condition
            selects[T] = select(T,condition)
        return selects
        
