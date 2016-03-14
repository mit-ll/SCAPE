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

import glob
from copy import deepcopy
from datetime import datetime
from typing import *

import networkx as nx

import scape.utils
from scape.utils.log import new_log
from scape.utils.decorators import (
    memoized_property,
)

import scape.config as config

from scape.registry import naming
from scape.registry.utils import (
    TaggedDimension,
)
from scape.registry.exceptions import (
    ScapeRegistryError,
)
from scape.registry.question import (
    Question,
)
from functools import reduce

_log = new_log('scape.registry.registry')

def read_registry_path(path: str) -> Any:
    reader = scape.utils.read_json
    if path.endswith('.yml'):
        reader = scape.utils.read_yaml
    return reader(path)

import ipaddress
class PandasWrapper(object):
    def __init__(self, registry, df, selection=None):
        self.df = df
        self.registry = registry
        columns = df.columns
        if not selection:
            _log.info('No selection provided. Guessing event'
                      ' associated with this pandas DataFrame')
            best_candidate = None
            best_overlap = 0
            for E in self.registry.events:
                overlap = len(E.fields(*columns))
                if overlap > best_overlap:
                    best_candidate = E
                    best_overlap = overlap
            if not best_candidate:
                raise ScapeRegistryError(
                    'No event found in this registry that has'
                    ' columns associated with this pandas DataFrame'
                )
            selection = best_candidate
        self.selection = selection

    def _get_fields(self, *tdims):
        fields = set(self.selection.fields.have_any(*tdims)['name'])
        return sorted(fields & set(self.df.columns))

    fields = None
    def __call__(self, *tdims):
        self.fields = self._get_fields(*tdims)
        return self

    def __eq__(self, other):
        if self.fields:
            if isinstance(other, ipaddress._BaseAddress):
                errors = (TypeError, ValueError)
                def crit(v):
                    return ipaddress.ip_address(v)==other
                try:
                    series = self.df[self.fields[0]].map(crit)
                except errors:
                    pass
                for f in self.fields[1:]:
                    try:
                        series |= self.df[f].map(crit)
                    except errors:
                        pass
            else:
                try:
                    series = self.df[self.fields[0]] == other
                except TypeError:
                    pass
                for f in self.fields[1:]:
                    try:
                        series |= self.df[f] == other
                    except TypeError:
                        pass
            return series

    def __getitem__(self, index):
        tdims = index
        if type(index) is str:
            tdims = [index]
        fields = set(self.selection.fields.have_any(*tdims)['name'])
        fields = sorted(fields & set(self.df.columns))
        # return self.df[fields]
        return fields
            

class Registry(object):
    '''Knowledge Registry configuration, access and query

    Default behavior is to create registry from Scape environment
    configuration
    
    >>> R = Registry(paths=['a.json','b.json'])
    >>> S = R.selection
    >>> S.events.has('source:')
    >>> Q = S('netflow').question(start='2014-08-02'
    >>> Q('source: = 192.168.1.1')

    '''

    def __init__(self, paths=None, dicts=None, registry_dict=None,
                 connection=None):
        paths = paths if paths else []

        # First load dictionaries from paths
        paths_dicts = [read_registry_path(p) for p in paths]

        # Then add individual dictionaries
        dicts = paths_dicts + dicts if dicts else paths_dicts

        # Last, add the gestalt registry_dict
        all_dicts = dicts + [registry_dict] if registry_dict else dicts

        self.registry_dict = scape.utils.merge_dicts(*all_dicts)
        
        self.graph = self.graph_from_dict(self.registry_dict)

        root_name = self.graph.node[self.graph.root_node]['name']
        self.root_name = root_name

        self.selection_class = type(
            'SelectionFor{}'.format(root_name.capitalize()),
            (Selection,), {'graph': self.graph},
        )
        self.selection_class.root_selection = self.selection_class()

        self.selection_class.question_class = type(
            'QuestionFor{}'.format(root_name.capitalize()),
            (Question,), {},
        )

        self.connection = connection

    def pickle(self):
        pass

    def __call__(self, *tdims):
        class SelectionPipe(object):
            registry = self
            def __init__(self, tdims, opstack=None):
                self.tdims = tdims
                self.opstack = opstack or []
            def __eq__(self, other):
                return SelectionPipe(self.tdims, self.opstack + [('eq',(self, other))])
            def __leq__(self, other):
                pass
            def __neq__(self, other):
                pass
            def __call__(self, df):
                if self.opstack:
                    print(self.opstack)
                F = self.registry.fields(*df.columns)
                fields = sorted(F.have_any(*self.tdims)['name'])
                return df[fields]
        # def df_pipe(df):
        #     F = self.fields(*df.columns)
        #     fields = sorted(F.have_any(*tdims)['name'])
        #     return df[fields]
        return SelectionPipe(tdims)

    _connection = None
    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self,connection):
        self._connection = connection
        self.selection_class.connection = connection

    @memoized_property
    def selection(self):
        return self.selection_class()

    @property
    def question(self):
        return self.selection.question
    
    @property
    def fields(self):
        return self.selection.fields
    @property
    def events(self):
        return self.selection.events
    @property
    def states(self):
        return self.selection.states
    @property
    def dims(self):
        return self.selection.dims
    @property
    def tags(self):
        return self.selection.tags

    @classmethod
    def graph_from_dict(cls, registry):
        T = naming.tag2node
        D = naming.dim2node
        E = naming.event2node
        S = naming.state2node
        F = naming.field2node

        registry = deepcopy(registry)

        G = nx.Graph()
        name = registry.pop('name',id(G))
        vis_stack = [registry.pop('vis','')]
        root_node = 'registry:{}'.format(name)
        G.root_node = root_node
        G.add_node(root_node,name=str(name))

        dimension_metadata = registry.pop('dimensions',{})
        tag_metadata = registry.pop('tags',{})
        em_items = [('event',(n,pdict))
                    for n,pdict in list(registry.pop('events',{}).items())]
        sm_items = [('state',(n,pdict))
                    for n,pdict in list(registry.pop('states',{}).items())]
        ntype_map = {'event': E, 'state': S}

        for ntype,(pname,pdict) in em_items+sm_items:
            pnode = ntype_map[ntype](pname)

            tags = pdict.pop('tags',[])

            pvstack = vis_stack + [pdict.pop('vis','')]

            fields = pdict.pop('fields',{})
            event_family = pdict.pop('family','raw')
            table = pdict.pop('table',naming.event2table(pname))
            timefield = pdict.pop('time','')
            if not timefield:
                raise ScapeRegistryError(
                    '"time" field must be provided for all Registry Events.'
                    ' Not provided for {}'.format(pdict)
                )

            config.expand_vars(pdict)

            G.add_node(pnode,name=pname,type=ntype,table=table,
                       time=timefield,family=event_family,
                       **pdict)
            G.add_edge(pnode,root_node)

            def add_tag(G,node,name):
                G.add_node(node,name=name,type='tag',
                           **tag_metadata.get(name,{}))
                G.add_edge(node,root_node)

            def add_dim(G,node,name):
                G.add_node(node,name=name,type='dim',
                           **dimension_metadata.get(name,{}))
                G.add_edge(node,root_node)

            for t in tags:
                tnode = T(t)
                if tnode not in G:
                    add_tag(G,tnode,name=t)
                    #G.add_node(tnode,name=t,type='tag')
                G.add_edge(tnode,pnode)

            for fname,fdict in list(fields.items()):    
                tags = fdict.pop('tags',[])
                dname = fdict.pop('dim',None)
                multiple = fdict.pop('multiple',False)
                family = fdict.pop('family',event_family)
                fvstack = pvstack + [fdict.pop('vis','')]
                fviz = '&'.join(['({})'.format(v)
                                 for v in [_f for _f in fvstack if _f]])
                fnode = F(pnode,fname)
                G.add_node(
                    fnode,name=fname,multiple=multiple,type='field',
                    family=family,vis=fviz,
                    **fdict)
                G.add_edge(pnode,fnode)
                G.add_edge(fnode,root_node)

                if dname:
                    dnode = D(dname)
                    if dnode not in G.node:
                        add_dim(G,dnode,name=dname)
                        #G.add_node(dnode,name=dname,type='dim',
                        #           **dimension_metadata.get(dname,{}))
                    G.node[fnode]['indexed'] = True
                    G.add_edge(fnode,dnode)

                for t in tags:
                    tnode = 'tag:'+t
                    add_tag(G,tnode,name=t)
                    #G.add_node(tnode,name=t,type='tag')
                    G.add_edge(fnode,tnode)

        for key, value in list(registry.items()):
            G.node[root_node][key] = value

        G = nx.freeze(G)
        return G

class Selection(object):
    # will be overridden by Registry object
    graph = None
    # same
    root_selection = None
    # same
    connection = None
    # same
    question_class = None

    def __init__(self, node_set=None):
        '''Selection of registry nodes (Events, States, Fields, Dimensions
        and/or Tags)

        RegistrySelections are subgraphs of the whole Registry graph

        Attributes:

          node_set (frozenset): set of Registry graph nodes that this
            Selection represents

          node_list (tuple): list version of node_set

          names (frozenset): Names of the nodes in node_set

          graph (NetworkX graph): NetworkX graph representation of the
            Registry

          type (str): Type of all the nodes in this Selection. Can be:
            "event", "state", "field", "dim", "tag" or "mixed"

          events
          states
          fields
          dims
          tags (Selection): for this Selection, return the
            events/states/fields/etc. that neighbor it

          tabular (Selection): For this Selection, return the
            neighboring nodes associated with storage in a database
            table (i.e. have the 'table' attribute). E.g. Field
            Selections' tabular neighbors are the Events/States they
            are associated with. Dim Selections' are the Events/States
            of the Fields they are assocaited with.

        >>>
        >>>

        '''
        if node_set is None:
            # Default select root node
            node_set = [self.graph.root_node]
        self.node_list = tuple(node_set)
        self.node_set = frozenset(node_set)

        vtypes = {v.split(':')[0] for v in self.node_list}
        if vtypes:
            if len(vtypes)==1:
                self.type = tuple(vtypes)[0]
            else:
                self.type = 'mixed'
        else:
            self.type = 'null'

        self.node = {}
        if self.node_list:
            self.node = self.graph.node[self.node_list[0]]

        self.names = frozenset(self['name'])

    @classmethod
    def from_node_set(cls,node_set):
        return cls(node_set)

    @classmethod
    def null_selection(cls):
        return cls([])

    @memoized_property
    def question(self):
        return self.question_class(selection=self) #pylint: disable=not-callable

    def pdf(self, *tdims):
        def df_pipe(df):
            F = sorted(self.fields.have(*tdims)['name'])
            return df[F]
        return df_pipe

    def __hash__(self):
        return hash(self.node_set)

    def __eq__(self, other):
        if isinstance(other,Selection):
            return (self.node_set == other.node_set)
        return NotImplemented
        
    def __str__(self):
        if self.type == 'mixed':
            return str(['{t}:{n}'.format(t=n.type,n=n['name'][0])
                        for n in self])
        else:
            return str(self['name'])

    def __repr__(self):
        return str(self)

    def draw(self,show_fields=False):
        import matplotlib.pyplot as plt
        import networkx as nx
        from mpld3 import plugins

        if show_fields:
            nodes = (self.node_set |
                     set(reduce(lambda a,b:set(a)|set(b),
                                [self.graph.neighbors(n)
                                 for n in self.node_set],
                                set())))
            nodes = {n for n in nodes if not n.startswith('registry:')}
            G = self.graph.subgraph(nodes)
        else:
            G = nx.Graph()
            if self.type in {'field','registry'}:
                return self.events.draw()
            else:
                if self.type in {'event'}:
                    for S in self:
                        sn = S.node_list[0]
                        G.add_node(sn,**S.node)
                        for D in S.fields.dims|S.tags|S.fields.tags:
                            dn = D.node_list[0]
                            G.add_node(dn,**D.node)
                            G.add_edge(sn,dn)
                elif self.type in {'dim'}:
                    for S in self:
                        sn = S.node_list[0]
                        G.add_node(sn,**S.node)
                        for E in S.fields.events|S.fields.tags|S.fields.events.tags:
                            dn = E.node_list[0]
                            G.add_node(dn,**E.node)
                            G.add_edge(sn,dn)
                elif self.type in {'tag'}:
                    for S in self:
                        sn = S.node_list[0]
                        G.add_node(sn,**S.node)
                        for E in S.events|S.fields.events|S.fields.dims:
                            dn = E.node_list[0]
                            G.add_node(dn,**E.node)
                            G.add_edge(sn,dn)
                    
        pos = nx.spring_layout(G)
        colors = []
        for n in G.nodes():
            vtype = n.split(':')[0]
            if vtype == 'field':
                colors.append('grey')
            elif vtype == 'event':
                colors.append('white')
            elif vtype == 'dim':
                colors.append('red')
            elif vtype == 'tag':
                colors.append('green')

        plt.rcParams["figure.figsize"] = (8,8)
        fig,ax = plt.subplots(subplot_kw={'xticks': [], 'yticks': []})
        nodes = nx.draw_networkx_nodes(G,pos=pos,node_color=colors)
        labels = nx.draw_networkx_labels(G,pos=pos,font_size=14,
                                         labels={n:G.node[n]['name']
                                                 for n in G.nodes()})
        edges = nx.draw_networkx_edges(G,pos=pos, edge_color='grey',width=4)
        # elist,labels = [],[] 
        # for (v0,v1) in G.edges():
        #     labels.append(nested_dict(G.edge[v0][v1]))
        # tooltip = plugins.PointHTMLTooltip(edges, labels,
        #                                    voffset=10, hoffset=10, css=css,
        #                                    )
        # plugins.connect(fig,tooltip)
        return 

    # memoize at class level
    _neighbors = {}             
    def _get_neighbors(self,vtype):
        neighbors = self._neighbors
        key = (vtype,frozenset(self.node_set))
        if key in neighbors:
            return neighbors[key]

        prefix = '{}:'.format(vtype)
        node_set = set()
        for v in self.node_list:
            node_set.update(
                [n for n in self.graph.neighbors(v)
                 if n.startswith(prefix)]
            )
        node = self.from_node_set(node_set)
        neighbors[key] = node
        return node

    @property
    def fields(self):
        return self._get_neighbors('field')
    @property
    def events(self):
        return self._get_neighbors('event')
    @property
    def states(self):
        return self._get_neighbors('state')
    @property
    def dims(self):
        return self._get_neighbors('dim')
    @property
    def tags(self):
        return self._get_neighbors('tag')

    _tabular_memo = {}
    def _tabular(self):
        key = frozenset(self.node_set)
        if key in self._tabular_memo:
            return self._tabular_memo[key]

        tabular = self.null_selection()
        if self.type in {'event','state'}:
            tabular = self
        elif self.type in {'field','registry'}:
            tabular = self.events|self.states
        elif self.type == 'dim':
            tabular = self.fields.events
        elif self.type == 'tag':
            tabular = self.fields.events|self.events

        self._tabular_memo[key] = tabular
        return tabular

    @property
    def tabular(self):
        '''Return the Registry nodes associated with this node that have the
        table attribute

        '''
        return self._tabular()

    @property
    def last_time(self):
        times = [_f for _f in self.connection.last_times(self.tabular) if _f]
        if times:
            return max(times)
        else:
            return datetime(1,1,1)

    # memoized at class level
    _has_memo = {}
    def _has(self, tagged_dims, any_or_all):
        tagged_dims = [TaggedDimension(d) for d in tagged_dims]

        # check for memoized value
        has_memo = self._has_memo
        memo_key = (any_or_all,frozenset(self.node_set), frozenset(tagged_dims))
        if memo_key in has_memo:
            return has_memo[memo_key]

        node_set = set()

        R = self.root_selection
        tagged_dims = [
            {
                'tags':{t:R.tags(t).names for t in td.tags},
                # if dimension is null, search space is all dimensions
                'dims':R.dims(td.dim).names,
            }
            for td in tagged_dims
        ]

        for select in self:
            if select.type == 'dim':
                fields = select.fields
                tags = (select.fields.tags|select.fields.events.tags).names
                dims = select.names

            elif select.type == 'field':
                fields = select
                tags = (select.tags|select.events.tags|
                        select.states.tags).names
                dims = select.dims.names

            elif select.type == 'tag':
                fields = select.fields|select.events.fields
                tags = select.names
                dims = (select.fields.dims|
                        select.events.fields.dims|
                        select.states.dims).names

            elif select.type in {'event','state'}:
                fields = select.fields
                tags = (select.fields.tags|select.tags).names
                dims = (select.fields.dims).names

            if any_or_all(
                    (any(
                        # any field has
                        
                        (
                            # all tags (field's and field's event's tags)
                            (all((not (f.tags|f.events.tags).names.isdisjoint(tset))
                                 for tset in list(td['tags'].values())))

                            and

                            # the dimension
                            (not f.dims.names.isdisjoint(td['dims']))
                            
                            # referenced in this tagged dimension
                        )

                        for f in fields
                    ))
                    for td in tagged_dims):
                node_set.update(select.node_set)


        new = self.from_node_set(node_set)
        has_memo[memo_key] = new
        return new

    def has(self, *tagged_dims):
        '''Subset of this selection that has all of the the given tagged
        dimensions

        >>> # Which events have the tag 'source' or have at least one
        >>> # field with the tag 'source'?
        >>> R.events.has('source:')
        ['proxy', 'netflow']
        >>> R.events.has('request:')
        ['proxy']

        >>> # Which fields have both the 'source' and 'internal' tags?
        >>> R.fields.has('source:internal:')
        ['src_ip', 'bytes_from_client']

        >>> # Which dimensions have a field associated with them that
        >>> # has both the 'source' and 'internal' tags?
        >>> R.dims.has('source:internal:')
        ['bytes', 'ipv4']

        >>> # Which events are associated with the fields that are of
        >>> # dimension 'domain' and have the tag 'referer'?
        >>> R.fields.has('referer:domain').events
        ['proxy']
        >>> R.fields.has('source:ip').events
        ['proxy', 'netflow']

        '''
        if self.type == 'registry':
            return self.tabular.has_all(*tagged_dims)
        return self._has(tagged_dims, all)
    has_all = has
    have_all = has
    have = have_all    

    def has_any(self, *tagged_dims):
        '''Subset of this selection that has any of the given tagged
        dimensions

        >>> R.events.has_all('source:','internal:')
        ['proxy']
        >>> R.events.has_any('source:','internal:')
        ['proxy', 'netflow']

        '''
        if self.type == 'registry':
            return self.tabular.has_any(*tagged_dims)
        return self._has(tagged_dims, any)
    have_any = has_any

    def __call__(self, *names, **attr_constraints):
        '''For this Selection, which of its members' names prefix-match the
        given names and whose attributes match the given constraints

        Args:

          *names (str) positional arguments of names to prefix-match
            to this Selection's node names

          **attr_constraints (str -> str) keyword arguments to filter
            the Selection's nodes by their attributes

        Returns: (Selection) Registry Selection matching the given
          constraints

        >> R.selection.dims('ip')
        ['ipv4', 'ipv6']
        >> R.selection.dims(name='ip')
        []
        >> R.selection.dims(name='ipv4')
        ['ipv4']

        '''
        if names:
            node_set = [v for v in self.node_set
                        if any([self.graph.node[v]['name'].startswith(n)
                                for n in names])]
        else:
            node_set = set(self.node_set)

        for attr, values in list(attr_constraints.items()):
            if isinstance(values, str):
                attr_constraints[attr] = [values]

        node_set = [v for v in node_set
                    if all(any(self.graph.node[v].get(attr)==value
                               for value in values)
                           for attr,values in list(attr_constraints.items()))]

        return self.from_node_set(node_set)

    def __bool__(self):
        return bool(self.node_set)

    def __len__(self):
        return len(self.node_set)

    def __getitem__(self, index):
        if isinstance(index, int):
            return self.from_node_set([self.node_list[index]])
        elif isinstance(index, str):
            return [self.graph.node[v][index] for v in self.node_list]

    def get(self, key, default=None):
        return [self.graph.node[v].get(key,default) for v in self.node_set]

    def __or__(self, other):
        return self.from_node_set(self.node_set|other.node_set)
    def __xor__(self, other):
        return self.from_node_set(self.node_set^other.node_set)
    def __and__(self, other):
        return self.from_node_set(self.node_set&other.node_set)
    def __sub__(self, other):
        return self.from_node_set(self.node_set - other.node_set)
    def __invert__(self):
        S = self.__class__()
        return (S.events|S.states|S.dims|S.tags|S.fields) - self

    def spark_op(self,function):
        conn_args = self.connection.pickle()
        nodes = self.node_set
        def wrapper(*args):
            from scape.registry.connection import ScapeConnection
            if wrapper.connection is None:
                wrapper.connection = ScapeConnection.unpickle(*conn_args)
            C = wrapper.connection
            S = C.registry.selection.from_node_set(nodes)
            retval = function(*(args+(S,)))
            return retval
        wrapper.connection = None
        return wrapper
