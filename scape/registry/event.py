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

''' Individual event abstraction object classes
'''
import itertools
import pprint
from collections import OrderedDict
from datetime import datetime

import networkx

import scape.utils
from scape.utils.decorators import (
    memoized_property,
)

from scape.registry.utils import (
    TaggedDimension, HashableDict,
)
from functools import reduce

class _EventView(object):
    def __init__(self,question):
        self.question = question

    @memoized_property
    def events(self):
        return self.question().events
    

class EventSet(_EventView):
    def __call__(self,*keys):
        return self.events.set(*keys)


class EventFlatSet(_EventView):
    def __call__(self,*keys):
        return self.events.flatset(*keys)

def nested_dict(D):
    tstr = '<table class="tool">'
    for k,v in sorted(list(D.items()),key=lambda e:e[0] if not e[0].isdigit() else int(e[0])):
        rstr = '<tr class="tool"><td class="tool">{}</td>'.format(k)
        if isinstance(v,dict):
            v = nested_dict(v)
        rstr += '<td class="tool">{}</td></tr>'.format(v)
        tstr += rstr
    tstr += '</table>'
    return tstr

css = """
table.tool
{
  border-collapse: collapse;
}
th.tool
{
  color: #ffffff;
  background-color: #000000;
}
td.tool
{
  background-color: #cccccc;
}
table.tool, th.tool, td.tool
{
  font-family:Arial, Helvetica, sans-serif;
  border: 1px solid black;
  text-align: right;
}
"""

class DiGraph(networkx.DiGraph):
    def draw(self, color=None):
        import matplotlib.pyplot as plt
        import networkx as nx
        from mpld3 import plugins
        pos = nx.shell_layout(self)
        colors = 'red'
        if color:
            colors = [color(n) for n in self.nodes()]

        plt.rcParams["figure.figsize"] = (11,12)
        fig,ax = plt.subplots(subplot_kw={'xticks': [], 'yticks': []})
        nodes = nx.draw_networkx_nodes(self,pos=pos,node_color=colors)
        labels = nx.draw_networkx_labels(self,pos=pos,font_size=14)
        edges = nx.draw_networkx_edges(self,pos=pos, edge_color='grey',width=4)
        elist,labels = [],[] 
        for (v0,v1) in self.edges():
            labels.append(nested_dict(self.edge[v0][v1]))
        tooltip = plugins.PointHTMLTooltip(edges, labels,
                                           voffset=10, hoffset=10, css=css,
                                           )
        plugins.connect(fig,tooltip)
        return 

class EventGraph(_EventView):
    def __call__(self,*keys):
        G = networkx.Graph()
        for event in self.events(*keys):
            nodes = set()
            for k in keys:
                for v in event[k]:
                    G.add_node(v,field=k)
                    nodes.add(v)
            for e0,e1 in itertools.combinations(nodes,2):
                if not G.has_edge(e0,e1):
                    G.add_edge(e0,e1,weight=0)
                G.edge[e0][e1]['weight'] += 1
        return G
            
class EventDiGraph(_EventView):
    def __call__(self,*key_pairs):
        key_pairs,edge_keys = list(zip(*[(p[:2],p[-1]) if len(p)>2 else (p,[]) for p in key_pairs]))
        keys = ( reduce(lambda a,b:set(a)|set(b),key_pairs,set()) |
                 reduce(lambda a,b:set(a)|set(b),edge_keys,set()) )
        #G = networkx.DiGraph()
        G = DiGraph()
        for event in self.events(*keys):
            for i,(k0,k1) in enumerate(key_pairs):
                ekeys = edge_keys[i]
                for v0,v1 in event.cartesian(k0,k1):
                    G.add_node(v0,field=k0)
                    G.add_node(v1,field=k1)
                    if not G.has_edge(v0,v1):
                        G.add_edge(v0,v1,weight=0)
                    for ekey in ekeys:
                        for evalue in event[ekey]:
                            G.edge[v0][v1].setdefault(ekey,{}).setdefault(evalue,0)
                            G.edge[v0][v1][ekey][evalue] += 1
                    G.edge[v0][v1]['weight'] += 1
        return G
            

class EventList(object):
    def __init__(self,question):
        self.question = question
        self.time_window = None

    def __len__(self):
        return len(self.events)

    def __repr__(self):
        return repr(self.events)
        
    def _repr_pretty_(self,p,cycle):
        if cycle:
            p.text('{...}')
        else:
            p.pretty(self.events)

    def _repr_html_(self):
        return '<p/>'.join([e._repr_html_() for e in self.events])
    
    def __str__(self):
        return str(self.events)

    _events = None
    @memoized_property
    def events(self):
        self.time_window = self.question.time_window
        events = list(self.question)
        return events

    def __call__(self,*keys):
        el = EventList(self.question)
        el.time_window = self.time_window
        el._events = [e(*keys) for e in self.events] #pylint: disable=protected-access
        return el
        
    def __getitem__(self,key):
        if isinstance(key,int):
            return self.events[key]
        else:
            return [e[key] for e in self.events]

    def set(self,*keys):
        events = list(set(self(*keys)))
        elist = EventList(self.question)
        elist.time_window = self.time_window
        elist._events = events
        return elist
        sets = []
        for k in keys:
            sets.append(self.flatset(k))
        return tuple(sets)

    def flatset(self,*keys):
        vitems = self[keys]
        values = set()
        for vtuple in vitems:
            if isinstance(vtuple[0],str):
                value.update(vtuple)
            else:
                for vt in vtuple:
                    values.update(vt)
        return values


class tcolors:                  # pylint: disable=no-init
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    GRAY = '\033[92m'
    GREY = GRAY
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    @classmethod
    def color(cls,text,color):
        return getattr(tcolors,color.upper()) + text + cls.END
        

class Event(object):
    _field2tdim = {}
    def __init__(self,row,selection):
        self.row = row
        self.selection = selection
    def __str__(self):
        return repr(self)
    def __repr__(self):
        return repr(self.pretty_row())
    def _repr_pretty_(self,p,cycle):
        # def dim(s):
        #     return '<span style="color:red">'+s+'</span>'
        if cycle:
            p.text('{...}')
        else:
            p.pretty(self.pretty_row())
    def _repr_html_(self):
        row = self.td_row()
        rows = []
        items = list(row.items())
        items.sort(key=lambda e:(e[0][1],e[0][2]))
        for (tags,dim,field),value in items:
            tstr = ' : '.join([
                    '<span style="color:green;font-family:monospace">{}</span>'.format(t)
                    for t in tags])
            dstr = '<span style="color:red;font-family:monospace">{}</span>'.format(dim)
            fstr = '<span style="color:grey;font-family:monospace">{}</span>'.format(field)
            vstr = '<span style="font-family:monospace">{}</span>'.format(value)
            rows.append([tstr,dstr,fstr,vstr])
        trs = [
            '<tr>{}</tr>'.format(' '.join('<td>{}</td>'.format(v)
                                          for v in r))
            for r in rows
            ]
        table = '<table>{}</table>'.format('\n'.join(trs))
        return table

    def td_row(self):
        fields = self.selection.fields(*list(self.row.keys()))
        row = OrderedDict()
        found_fields = set()
        for F in fields:
            name = F.node['name']
            dim = ''
            if F.dims:
                dim = F.dims.node['name']
            tags = []
            if F.tags:
                tags = sorted(F.tags.names)
            tags = tuple(tags)
            if name in self.row:
                found_fields.add(name)
                row[(tags,dim,name)] = self.row[name]
        for unknown_field in set(self.row.keys())-found_fields:
            row[((),'',unknown_field)] = self.row[unknown_field]
        return row

    def pretty_row(self,dim_colorer=lambda s:s, tag_colorer=lambda s:s,
                   field_colorer=lambda s:s):
        td_row = self.td_row()
        pretty = {}
        for (tags,dim,field),value in list(td_row.items()):
            key = '{} : [{}] @{}'.format(
                ' : '.join([tag_colorer(t) for t in tags]),
                dim_colorer(dim),
                field_colorer(field),
                )
            pretty[key] = value
        return pretty

    def __hash__(self):
        return hash(HashableDict(self.row))
    def __eq__(self,other):
        if isinstance(other,Event):
            return HashableDict(self.row) == HashableDict(other.row)
        return self == other

    _connection = None
    @classmethod
    def unpickle(cls, row, connection):
        from scape.registry.connection import Connection
        if cls._connection is None:
            cls._connection = Connection.unpickle(*connection)
        selection = cls._connection.registry.selection_class.from_node_set(
            row.get('__scape_selection__',[])
        )
        return cls(row,selection)
        
    def pickle(self):
        row = self.row.copy()
        row['__scape_selection__'] = self.selection.node_list
        return row

    @memoized_property
    def datetime(self):
        if 'scape_timestamp' in self.row:
            return datetime.strptime(self.row['scape_timestamp'],
                                     '%Y-%m-%d %H:%M:%S')
        return scape.utils.date_convert(self.row[self.selection['time'][0]])

    def raw_update(self,**kw):
        row_update =  {}
        if 'scape_rowid' in self.row:
            row_update['scape_rowid'] = self.row['scape_rowid']
        row_update.update(kw)
        self.selection.ingest_rows([row_update])

    def update(self,**kw):
        row_update =  {}
        if 'scape_rowid' in self.row:
            row_update['scape_rowid'] = self.row['scape_rowid']
        for td,v in list(kw.items()):
            if td.startswith('@'):
                row_update[td[1:]] = v
            else:
                for F in self.selection.fields.has(td):
                    row_update[F.node['name']] = v
        self.selection.ingest_rows([row_update])

    def __contains__(self, key):
        key = self._resolve_key(key)
        if isinstance(key,str):
            # direct field access
            return key in self.row
        else:
            # key is either a tuple of field Selections or a Selection
            return bool(key)

    _key_lut = {}
    def _resolve_key(self, key):
        lut = self._key_lut.setdefault(self.selection,{})
        if key in lut:
            return lut[key]

        if isinstance(key,str):
            if key.startswith('@'):
                fname = key[1:]
                lut[key] = [(fname,self.selection.fields(name=fname))]
            else:
                lut[key] = [(f.node['name'],f) for f in self.selection.fields.has(TaggedDimension(key))]
        elif isinstance(key,TaggedDimension):
            lut[key] = [(f.node['name'],f) for f in self.selection.fields.has(key)]
        else:
            raise AttributeError(
                'keys must be strings or TaggedDimension objects'
            )
        return lut[key]

    def __call__(self, *keys):
        field_sets = [self._resolve_key(k) for k in keys]
        row = {}
        for fields in field_sets:
            for name,_ in fields:
                if name in self.row:
                    row[name] = self.row[name]
        return Event(row,self.selection)

    def cartesian(self,*keys):
        values = [self[k] for k in keys if self[k]]
        return itertools.product(*values)
        
    def __getitem__(self, key):
        if isinstance(key,list):
            key = tuple(key)
        if isinstance(key,tuple):
            return tuple(self[k] for k in key)
            
        key = self._resolve_key(key)
        if key:
            fields = key
            return tuple(
                self.row[n]
                for n,f in fields if n in self.row
            )
        else:
            return ()
            
