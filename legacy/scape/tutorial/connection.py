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
import datetime

from acqua.connection import Connection

from scape.utils.decorators import (
    memoized_property
)
from scape.registry.registry import Registry

from scape.tutorial import data

__dir__ = os.path.abspath(os.path.dirname(__file__))

class TutorialConnection(Connection):
    def __init__(self, date=None, **kw):
        if 'log_level' not in kw:
            kw['log_level'] = 'info'
        super(TutorialConnection,self).__init__(**kw)
        date = date or (datetime.date.today() - datetime.timedelta(days=1))
        rows = {}

        rows['dhcp'],rows['addc'],rows['proxy'] = data.generate_rows(date,0)

        R = self.registry
        for T in R.selection.tabular:
            name = T.node['name']
            T.create_tables()
            T.ingest_rows(rows[name])
            
    @memoized_property
    def registry(self):
        paths = glob.glob(os.path.join(__dir__,'registry','*.json'))
        return Registry(paths=paths,connection=self)
