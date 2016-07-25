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
import inspect
import re
import pprint
from datetime import date, datetime, timedelta

import lrparsing
from lrparsing import (
    # Keyword,
    # THIS,
    List, Prio, Ref, Token, Tokens
)

import scape.utils
from scape.utils import lines
from scape.utils.args import (
    date_convert, 
)
from scape.utils.log import new_log
from scape.utils.decorators import (
    memoized_property,
    watch_property
)

from scape.registry.exceptions import (
    ScapeRegistryError, ScapeTimeError,
)
from scape.registry.utils import (
    TaggedDimension,
)

from scape.registry.operators import (
    RegistryOperator, OperatorSequence
)

from scape.registry.condition import (
    RegexCondition, EqualsCondition, 
)
 
from scape.registry.event import (
    EventSet, EventFlatSet, EventGraph, EventDiGraph, EventList, Event,
)

import scape.registry.spark

_log = new_log('scape.registry.question')

_re_type = type(re.compile(''))

'''
>>> Q("source:ip = 192.168.1.1 & dest:ip = (192.2.1.30, 201.2.55.104))")
'''
class QuestionParser(lrparsing.Grammar):
    #
    # Put Tokens we don't want to re-type in a TokenRegistry.
    #
    class T(lrparsing.TokenRegistry): # pylint:disable=abstract-method
        var = Token(re=r"{[A-Za-z_][A-Za-z_0-9.]*}")
        regex = Token(re=r"//.*?(?<!\\)//")
        quote = Token(re=r"\".*\"")
        ident = Token(re=r'(?:[\w\-_*./]+\s*)*[\w\-_*./]+')

    #
    # Grammar rules.
    #
    expr = Ref("expr")                # Forward reference
    tagged_dim = List(T.ident,':',opt=True)
    field = '@'+T.ident
    #raw = T.ident
    var = T.var
    quote = T.quote
    #regex = '/' + T.ident + '/'
    regex = T.regex
    value = T.ident | var | regex | quote
    tup = '(' + List(value,',',opt=True) + ')' # | '[' + List(value,',',opt=True) + ']'
    eq_rhs = (tup | value)
    equal = tagged_dim + '=' + eq_rhs | field + '=' + eq_rhs
    notequal = tagged_dim + '!=' + eq_rhs | field + '!=' + eq_rhs
    paren_expr = Token('(') + expr + Token(')')
    atom = field | tagged_dim | equal | notequal | paren_expr
    and_ = expr << Tokens('& ,') << expr
    or_ = expr << '|' << expr
    not_ = '!' >> expr
    expr = Prio(                      # If ambiguous choose atom 1st, ...
        atom,
        not_,
        and_ | or_ ,
        # or_,
        # Token('not') >> THIS,
        # THIS << Tokens('and or') << THIS,
    )
    START = expr                      # Where the grammar must start
    COMMENTS = (                      # Allow C and Python comments
        Token(re="#(?:[^\r\n]*(?:\r\n?|\n\r?))") |
        Token(re="/[*](?:[^*]|[*][^/])*[*]/"))

def test(expr):
    #print lrparsing.repr_grammar(QuestionParser)
    parse_tree = QuestionParser.parse(expr)
    print(QuestionParser.repr_parse_tree(parse_tree))
    return parse_tree

class QuestionHandler(object):
    '''Handles parse events generate by QuestionParser grammar, produces a
    dictionary of {tabular Selection(obj): Condition}

    For each token defined in QuestionParser, if a
    "handle_[token_name]" method is provided, this method is called
    during the parse tree traversal.

    Leaf conditions (currently "equals" and "regex") 

    '''
    def __init__(self, question, selection):
        self.question = question
        self.selection = selection

    @watch_property('question')
    def parse_tree(self):
        return QuestionParser.parse(self.question)

    def parse(self):
        outcome = self.handle(self.parse_tree)
        
        if isinstance(outcome,TaggedDimension):
            # Handler can return a single tagged dimension
            return {outcome}, lambda *a:{}
        elif isinstance(outcome,(list,tuple)):
            # Usually handler returns either a tuple of "bare" tagged
            # dimensions, a tuple of equality/regex conditions, or a
            # mixture of both
            all_tagged_dims, resolver = outcome
            return all_tagged_dims, resolver
        else:
            raise NotImplementedError()
        
    def handle(self,node):
        rule = node[0]
        handle_cb = 'handle_{}'.format(rule.name)
        if hasattr(self,handle_cb):
            return getattr(self,handle_cb)(node)
        raise NotImplementedError(handle_cb)

    def handle_START(self,node):
        return self.handle(node[1])

    def handle_expr(self,node):
        return self.handle(node[1])

    def handle_paren_expr(self,node):
        expr = node[2]
        return self.handle(expr)
        
    def handle_atom(self,node):
        return self.handle(node[1])

    def handle_tup(self, node):
        if node[-2][1] == ',':
            # trim trailing commas
            parts = node[2:-1:2]
        else:
            parts = node[2::2]

        values = reduce(lambda a,b:a+b,map(self.handle,parts))
        return values

    def handle_var(self,node):
        def expand_var(variable_name):
            # remove leading and trailing braces
            variable_name = variable_name[1:-1]
            variable_value = None
            try:
                current_frame = inspect.currentframe()
                found = False
                # Ignore current frame (expand_var) and previous
                # (names_to_kw)
                frames = inspect.getouterframes(current_frame)[2:]
                for (frame,_,_,_,_,_) in frames:
                    if variable_name in frame.f_locals:
                        variable_value = frame.f_locals[variable_name]
                        found = True
                        break
                if not found:
                    raise ScapeRegistryError(
                        'Could not find variable "{}"'.format(variable_name)
                    )
            except ScapeRegistryError:
                raise
            finally:
                # explicitly clean up code frames
                del current_frame
                for f in frames:
                    del f
                del frames
            return variable_value

        value = expand_var(node[1][1])
        # value could be a sequence of things
        if not isinstance(value,(list,tuple,set)):
            value = [value]
        return value

    def handle_regex(self, node):
        value = node[1][1]
        # strip leading and trailing slashes
        value = value[2:-2]
        return [re.compile(value)]

    def handle_value(self,node):
        if node[1][0].name == 'T.ident':
            value = [node[1][1]]
        else:
            value = self.handle(node[1])
        return value

    def handle_quote(self,node):
        value = [node[1][1]]
        return value

    def handle_field(self,node):
        field = node[2][1]
        return field

    def handle_tagged_dim(self,node):
        value = ''.join([v[1] for v in node[1:]])
        tdim = TaggedDimension(value)
        return tdim
        
    def handle_eq_rhs(self,node):
        return self.handle(node[1])

    def handle_equal(self,node):
        # The left-hand side (lhs) is either a raw field (@field_name)
        # or a tagged dimension (tag1:tag2:dim)
        lhs = self.handle(node[1])

        if isinstance(lhs, basestring):
            # it's a field
            tagged_dim = None
            field = lhs
        else:
            # it's a tagged dimension
            tagged_dim = lhs
            field = None

        # The right-hand side (rhs) is either a single value or a
        # tuple of values. These values can be raw strings, strings
        # with wildcards, or variables.
        #
        # In all cases, the handlers for each of these should return a
        # sequence of values (even if the rhs is only a single value).
        values = self.handle(node[3])
        values = list(values)

        for i,value in enumerate(values):
            condition_class = EqualsCondition
            if isinstance(value,_re_type):
                # If the value was a variable that resolved to a
                # compiled regex, then it is a RegexCondition
                value = value.pattern
                condition_class = RegexCondition
            else:
                if not isinstance(value,basestring):
                    # If it was a variable that resolved to a
                    # non-string (integers, etc.), then turn it into a
                    # string
                    value = str(value)
                
                if value[0] == '"' and value[-1] == '"':
                    # Exact string
                    value = value[1:-1]
                elif '*' in value:
                    # Strings with wildcards are just turned into
                    # regexes where each * becomes a ".*", and
                    # the regex is treated as a whole string
                    # (i.e. not a substring). 
                    regex = re.compile(value.replace('*','.*'))
                    # If substring searching behavior is desired, then
                    # append/prepend a wildcard
                    value = '^'+regex.pattern+'$'
                    condition_class = RegexCondition
            values[i] = (value,condition_class)

        def resolver(all_tagged_dims,tdim=tagged_dim,
                     field=field, # pylint:disable=dangerous-default-value
                     values=values):
            conditions = {}
            if field is not None:
                for T in self.selection.tabular.fields(name=field).events:
                    for F in T.fields(name=field):
                        cobjs = []
                        for value, condition_class in values:
                            cond_obj = condition_class(F.node['name'], value)
                            cobjs.append(cond_obj)
                        condition = reduce(lambda a,b:a|b,cobjs)
                        conditions.setdefault(T,[]).append(condition)
            else:
                for T in self.selection.tabular.has(*(all_tagged_dims|{tdim})):
                    for F in T.fields:
                        if F.has(*[tdim]):
                            fname = F.node['name']
                            cobjs = []

                            for value,condition_class in values:
                                cond_obj = condition_class(fname,value,tdim)
                                cobjs.append(cond_obj)

                            condition = reduce(lambda a,b:a|b,cobjs)
                            conditions.setdefault(T,[]).append(condition)

            return {k:reduce(lambda a,b:a|b,v) for k,v in conditions.items()}

        if field is not None:
            tagged_dims = set()
        else:
            tagged_dims = set([tagged_dim])
        return tagged_dims, resolver

    def handle_notequal(self,node):
        tagged_dims, resolver = self.handle_equal(node)
        def notresolver(all_tagged_dims,resolver=resolver):
            conditions = resolver(all_tagged_dims)
            for T, condition in conditions.items():
                conditions[T] = ~condition
            return conditions
        return tagged_dims, notresolver

    def _handle_binary_boolean(self,node,operation):
        # Binary boolean operator <and '&' ','>  <or '|'> handler
        lhs, rhs = self.handle(node[1]), self.handle(node[3])
        all_tagged_dims = set()
        resolvers = {}

        for sname, side in [('lh',lhs),('rh',rhs)]:
            # side should either be a "bare" tagged dimension or a
            # (tagged dimensions, resolver) pair
            if isinstance(side,TaggedDimension):
                # tagged dim with no equality
                all_tagged_dims.add(side)

                # resolves to no conditions
                resolvers[sname] = lambda *a:{}
            else:
                # (tagged dimension, resolver) pair
                tagged_dims,reslvr = side
                all_tagged_dims.update(tagged_dims)

                resolvers[sname] = reslvr

        def resolver(all_tagged_dims,resolvers=resolvers.copy(),
                     operation=operation):
            # call resolvers with tagged dimensions for each side of
            # the boolean (lhs, rhs)
            all_conditions = {sname:res(all_tagged_dims)
                              for sname,res in resolvers.items()}

            # merge the two sides, starting with the lhs
            conditions = all_conditions['lh']
            
            for T, condition in all_conditions['rh'].items():
                if T not in conditions:
                    conditions[T] = condition
                else:
                    conditions[T] = operation(conditions[T],condition)

            return conditions
                    
        return all_tagged_dims, resolver

    def handle_and_(self,node):
        return self._handle_binary_boolean(node,lambda a,b:a&b)

    def handle_or_(self,node):
        return self._handle_binary_boolean(node,lambda a,b:a|b)

    def handle_not_(self,node):
        expr = self.handle(node[2])
        all_tagged_dims = set()
        if isinstance(expr,TaggedDimension):
            all_tagged_dims.add(expr)
            resolver = lambda *a:{}
        else:
            tagged_dims,res = expr
            all_tagged_dims.update(tagged_dims)
            def resolver(all_tagged_dims, res=res):
                conditions = {}
                for T,condition in res(all_tagged_dims).items():
                    conditions[T] = ~condition
                return conditions
        return all_tagged_dims, resolver


class Question(OperatorSequence):
    '''Question docs

    '''

    def __init__(self, selection):
        '''Question generator for a given selection. 

        Args:

          selection (Selection): Selection object (i.e. tabular scope)
            for this question generator

        Normal use-case is to use the Registry/Selection API to access
        question objects. 

        >>> R = Registry()

        For questions over the scope of all tabular elements in R:

        >>> Q = R.question # default root selection (i.e. everything)
        equivalent to 
        >>> Q = R.selection.question
        
        E.g. For questions over the scope of only those tabular
        elements (e.g.: Events, States) with (internal IP, external
        IP) relationships defined:

        >>>
        >>> Q = R.selection.has('internal:ip','external:ip').question
        >>>

        For questions over the scope of Events with tag "netflow"
        >>>
        >>> Q = R.selection.tags('netflow').events.question

        '''
        super(Question,self).__init__(self)
        self.selection = selection
        self.question = None

    def rdd(self, sc, max_bin_size=10000,granularity='minute'):
        rdelta = scape.utils.get_rdelta(granularity)

        counts = None
        if any(c for c in self.conditions.values()):
            if all(not bool(c.regex_conditions)
                   for c in self.conditions.values()):
                counts = {}
                val_counts = self.value_counts(granularity)
                for _,vdict in val_counts.items():
                    for value in vdict:
                        for dt,cnt in vdict[value].items():
                            c = counts.setdefault(dt,0)
                            counts[dt] = c + cnt
        if counts is None:
            counts = self.row_counts(granularity)

        fudge = max_bin_size/10.
        def do_add(count,size):
            return (size+count < max_bin_size+fudge)

        dts = sorted(counts)
        B = [[dts[0],dts[0]+rdelta,counts[dts[0]]]]

        for dt in dts[1:]:
            cnt = counts[dt]
            slot = B[-1]
            if do_add(cnt,slot[-1]):
                slot[-2] = dt+rdelta
                slot[-1] += cnt
            else:
                B.append([dt,dt+rdelta,cnt])
                    
        _log.debug(lines(pprint.pformat(B)))
        
        conn_pickle = self.selection.connection.pickle()
        sel_nodes = self.selection.node_list

        B = [(slot[0],slot[1],conn_pickle,sel_nodes,self.question)
             for slot in B]

        _log.debug(lines(pprint.pformat(B)))

        return sc.parallelize(B).flatMap(scape.registry.spark.load_events)


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

    def row_counts(self,granularity='minute'):
        '''Get number of events/rows for this Question's Selection during
        Question's time period at the given time granularity

        '''
        granularity = scape.utils.get_rdelta(granularity)
        start,end = self.time_window
        return self.selection.connection.row_counts(
            self.selection.tabular, start, end, granularity,
        )

    def dim_counts(self,granularity='minute'):
        '''Get number of events/rows for the tagged dimensions in this
        Question during its time period at the given time granularity

        '''
        granularity = scape.utils.get_rdelta(granularity)
        start,end = self.time_window
        tagged_dims = self.tagged_dimensions
        return self.selection.connection.dim_counts(
            self.selection.tabular, start, end, granularity, tagged_dims,
        )

    def value_counts(self,granularity='minute'):
        ''' Get 
        '''
        conditions = self.conditions
        start,end = self.time_window
        granularity = scape.utils.get_rdelta(granularity)
        return self.selection.connection.value_counts(
            conditions, start, end, granularity,
        )
        

    @property
    def unique(self):
        ''' Get 
        '''
        tagged_dims = self.tagged_dimensions
        start,end = self.time_window
        tabular = self.selection.tabular.fields.has_any(*tagged_dims).tabular
        return self.selection.connection.unique(
            tabular, start, end, tagged_dims,
        )

    @property
    def unique_sort(self):
        count_lut = self.unique
        for td,lut in count_lut.items():
            count_lut[td] = sorted(lut.items(),key=lambda e:[e[-1]])
        return count_lut
    sunique = unique_sort
        

    _start = timedelta(minutes=-15)
    @property
    def start(self):
        '''Valid values for start:

        datetime/str: start is this datetime

        timedelta/str: start is end - timedelta

        '''
        start = self._start
        if isinstance(start,timedelta):
            if start.total_seconds() < 0:
                start = timedelta(seconds=-start.total_seconds())
            start = self.end - start
        return start

    @start.setter
    def start(self, start):
        if isinstance(start,(basestring,date,datetime,timedelta)):
            start = date_convert(start)
        else:
            raise ScapeTimeError(
                '({}) is not a valid end time specifier. Start must be either a datetime or timedelta object (or the string representation of either)'.format(repr(start))
            )
        self._start = start

    _end = "now"
    @property
    def end(self):
        '''Valid values for end:

        "now": resolves to latest timestamp for this Selection

        Selection: resolves to latest timestamp for given Selection

        datetime/str: end is this datetime

        timedelta/str: end is
           [start + timedelta (if start is datetime)]
             or
           [now +/- timedelta (else)]

        '''
        end = self._end
        if end == 'now':
            end = self.selection.last_time
        elif isinstance(end,self.selection.__class__):
            end = end.last_time

        if isinstance(end,timedelta):
            if isinstance(self._start,datetime):
                if end.total_seconds() < 0:
                    end = timedelta(seconds=-end.total_seconds())
                end = self.start + end
            else:
                end = self.selection.last_time + end

        return end
                
    @end.setter
    def end(self, end):
        if isinstance(end,basestring):
            if end != 'now':
                end = date_convert(end)
        elif isinstance(end,(date,datetime,timedelta)):
            end = date_convert(end)
        elif isinstance(end,self.selection.__class__):
            pass
        else:
            raise ScapeTimeError(
                '({}) is not a valid end time specifier. End must be either "now", a Selection to pull a last time from, a datetime or timedelta object (or the string representation of either)'.format(repr(end))
            )

        self._end = end
    
    _center = None
    @property
    def center(self):
        '''If center is:

        timdelta/str: start and end will resolve normally and then be
            transformed by center delta

        '''
        return self._center
    @center.setter
    def center(self, center):
        if isinstance(center,(basestring,date,datetime,timedelta)):
            center = date_convert(center)
        elif center is not None:
            raise ScapeTimeError(
                '({}) is not a valid center time specifier. Center must be either None, a datetime or timedelta object (or the string representation of either)'.format(repr(center))
            )
        self._center = center

    @property
    def time_window(self):
        start,end = self.start, self.end
        center = self.center
        if isinstance(center, timedelta):
            start += center
            end += center
        elif isinstance(center,datetime):
            delta = timedelta(seconds=(end-start).total_seconds()/2.)
            start = center - delta
            end = center + delta
        return [start,end]

    def __call__(self,question=None, **time_args):
        '''Given some selection of the Registry, use tagged dimensions to
        answer questions about the data, similar to a WHERE clause in
        SQL. 

        Args:

          question (str) the question to search the data with

          start (datetime/timedelta/str) start time specification

          end (datetime/timedelta/str) end time specification

          center (datetime/timedelta/str) specifications for center of
              time window


        Let's find all events associated with the host at 192.168.1.1
        where they've requested an IP address directly (as opposed to
        a domain like "google.com")
        >>> 
        >>> Q = R.selection.question
        >>> IP = scape.utils.regex.IP.pattern
        >>> Q("internal:ip=192.168.1.1 & request:domain=/{IP}/").events
        >>>

        Raw fields can be looked up, as well
        >>>
        >>> Q("@src_ip=192.168.1.1").events
        >>>

        >>> Q = R.selection.question
        >>> Q.start = '-1h'
        >>> Q('source:ip=192.168.1.1',start='-15m')

        '''
        Q = self.__class__(self.selection)
        Q.question = question if question else self.question

        Q.start = self._start
        Q.end = self._end
        Q.center = self._center
        Q.operator_sequence = self.operator_sequence[:]

        for kw in ['start','end','center']:
            if kw in time_args:
                setattr(Q,kw,time_args[kw])

        return Q

    def __eq__(self, question):
        return (self.question == question.question,
                self.selection == question.selection)

    def __or__(self, other):
        if isinstance(other,Question):
            question = other
            Q = self.__class__(self.selection | question.selection)
            if self.question and question.question:
                Q.question = '({})|({})'.format(self.question,question.question)
            elif self.question:
                Q.question = self.question
            elif question.question:
                Q.question = question.question
            Q.start = min(self.start,question.start)
            Q.end = max(self.end,question.end)
            Q.operator_sequence = self.operator_sequence[:]
            return Q
        elif isinstance(other,(OperatorSequence,RegistryOperator)):
            operators = self.operator_sequence[:]
            if isinstance(other, OperatorSequence):
                operators.extend(other.operator_sequence)
            elif isinstance(other, RegistryOperator):
                operators.append(other.copy())
            for op in operators:
                op.selection = self.selection
            Q = self(self.question)
            Q.operator_sequence = operators
            return Q
        else:
            return NotImplemented

    def __and__(self, question):
        if isinstance(question,Question):
            Q = self.__class__(self.selection & question.selection)
            if self.question and question.question:
                Q.question = '({})&({})'.format(self.question,question.question)
            elif self.question:
                Q.question = self.question
            elif question.question:
                Q.question = question.question
            Q.start = min(self.start,question.start)
            Q.end = max(self.end,question.end)
            Q.operator_sequence = self.operator_sequence[:]
            return Q
        else:
            return NotImplemented

    def __invert__(self):
        if self.question:
            Q = self('!({})'.format(self.question))
        else:
            Q = self
        return Q

    @watch_property('question')
    def tagged_dimensions(self):
        if self.question:
            all_tagged_dims, _ = QuestionHandler(
                self.question,self.selection,
            ).parse()
            return all_tagged_dims
        return set()

    @watch_property('question')
    def conditions(self):
        if self.question:
            all_tagged_dims, resolver = QuestionHandler(
                self.question,self.selection,
            ).parse()
            return resolver(all_tagged_dims)
        else:
            return {T:None for T in self.selection.tabular}

    # @property
    # def tabular_operators(self):
    #     # This should return a dictionary of {tabular Selection:
    #     # Select object}
    #     conditions = self.conditions
    #     start,end = self.time_window
    #     return self.selection.connection.selects(
    #         start, end, conditions
    #     )

    @memoized_property
    def generator(self):
        # # Associate this question's selection with all operators in
        # # the operator sequence (so they can correctly resolve the
        # # tagged dimensions referenced in them)
        # for op in self.operator_sequence:
        #     op.selection = self.selection

        conditions = self.conditions
        start,end = self.time_window
        selects = self.selection.connection.selects(
            start, end, conditions
        )
        for T, select in selects.items():
            # Not implementing Registry-aware Operators until a
            # better, more general solution is found
            # # Add the all the (relevant) operators to each Select
            # # object
            # for op in self.operator_sequence:
            #     # An operator might not be relevant to a particular
            #     # tabular selection (i.e. a particular Event might not
            #     # have the tagged dimensions referenced in the
            #     # operator)
            #     tab_op = op.tabular_operators.get(T)
            #     if tab_op:
            #         select |= tab_op

            # The select object is an interable that should return a
            # sequence of rows
            for row in select:
                yield Event(row,T)
                    
        
    def next(self):
        return next(self.generator)

    @property
    def events(self):
        return EventList(self)

    @property
    def graph(self):
        return EventGraph(self)

    @property
    def digraph(self):
        return EventDiGraph(self)

    @property
    def set(self):
        return EventSet(self)
    
    @property
    def flatset(self):
        return EventFlatSet(self)

