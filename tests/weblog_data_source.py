# Copyright (2016) Massachusetts Institute of Technology.
# Reproduction/Use of all or any part of this material shall
# acknowledge the MIT Lincoln Laboratory as the source under the
# sponsorship of the US Air Force Contract No. FA8721-05-C-0002.

# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

from scape.registry.tagged_dim import tagged_dim
from scape.registry.condition import (
    Equals, MatchesCond, TrueCondition, 
    GreaterThan, GreaterThanEqualTo, 
    And, Or, 
)
from scape.registry.table_metadata import TableMetadata
from scape.registry.data_source import DataSource

weblog_metadata = TableMetadata({
    'clientip' : tagged_dim('client:ip'),
    'serverip' : tagged_dim('server:ip'),
    'url' : tagged_dim('http:url'),
    'status_code' : tagged_dim('http:status_code'),
    'time': tagged_dim('sec')
})

weblog_data = [ {
    'clientip' : '1.2.3.4',
    'serverip' : '4.4.4.4',
    'url' : 'http://foo.bar.com/index.html',
    'status_code' : '404',
    'time' : '03-31-2011 08:11:22'
},  {
    'clientip' : '7.8.9.2',
    'serverip' : '4.4.4.4',
    'url' : 'http://foo.bar.com/status.html',
    'status_code' : '200',
    'time' : '03-31-2011 08:14:33'
}, {
    'clientip' : '1.2.3.4',
    'serverip' : '4.4.4.4',
    'url' : 'http://quux.com/index.html',
    'status_code' : '200',
    'time' : '03-31-2011 09:23:11'
}, {
    'clientip' : '1.2.3.4',
    'serverip' : '4.4.4.5',
    'url' : 'http://biz.com/index.html',
    'status_code' : '200',
    'time' : '04-01-2011 01:10:11'
}]


def interpret(cond):
    if isinstance(cond, Equals):
        return lambda r: r[cond.lhs.name]==cond.rhs
    elif isinstance(cond, GreaterThan):
        return lambda r: r[cond.lhs.name]>cond.rhs
    elif isinstance(cond, GreaterThanEqualTo):
        return lambda r: r[cond.lhs.name]>=cond.rhs
    elif isinstance(cond, MatchesCond):
        return lambda r: re.match(cond.rhs, r[cond.lhs.name])
    elif isinstance(cond, And):
        return lambda r: all([interpret(c)(r) for c in cond.parts])
    elif isinstance(cond, Or):
        return lambda r: any([interpret(c)(r) for c in cond.parts])
    elif isinstance(cond, TrueCondition):
        return lambda r: True
    else:
        raise ValueError("Unexpected condition {}".format(str(cond)))

class PythonDataSource(DataSource):
    def __init__(self, metadata, data):
        super(PythonDataSource, self).__init__(metadata, "description", {
            '==': Equals,
            '=~':  MatchesCond
        })
        self._data = data

    def run(self, select):
        self.check_select(select)

        field_names = self._field_names(select)
        def proj(x):
            return {k:v for k, v in x.items() if k in field_names}

        cond = self._rewrite(select._condition)
        f = interpret(cond)
        return [proj(f) for f in filter(f, self._data)]

def get_weblog_ds():
    ds =  PythonDataSource(weblog_metadata, weblog_data)
    return ds

def get_auth_ds():
    ds = PythonDataSource(
        TableMetadata({
            'user': 'supplied:user',
            'server': 'dst:ip',
            'authserver': 'fqdn',
            'time' : 'sec'
        }), data=[])
    ds._name = "auth"
    return ds
