"""Scape Registry."""
import copy
import re
import json
from six import string_types # python pos
from collections import namedtuple

class TagsDim(object):
    """ A field selector containing any number of tags and an optional dimension.
    """
    def __init__(self, tags=None, dim=None):
        """ Create a TagsDim from an optional list of tags and an optional dimension. """
        if tags is None:
            tags = []
        for t in tags:
            if not isinstance(t, Tag):
                raise ValueError("Expecting a Tag, not " + str(t) + ' ' + str(type(t)))
        if dim and not isinstance(dim, Dim):
            raise ValueError("Expecting a Dim, not " + str(dim) + ' ' + str(type(dim)))
        self._tags = frozenset(tags)
        self._dim = dim

    def __repr__(self):
        dstr = self._dim.__repr__() if self._dim else "None"
        return "TagsDim(" + self._tags.__repr__() + ", " + dstr + ")"

    @property
    def tags(self):
        """ The collection of tags"""
        return self._tags

    @property
    def dim(self):
        """ The dimension or None"""
        return self._dim

    def __eq__(self, other):
        return type(self) == type(other) and (self.tags == other.tags) and (self.dim == other.dim)

    def __hash__(self):
        return hash((self.dim, self.tags))

    def to_dict(self):
        """ Convert a TagsDim() to a dict. """
        return {'tags' : [t.name for t in self.tags], 'dim' : self.dim._dim if self.dim else None }

    def _as_trs(self):
        r = ['<td>']
        if self._dim:
            r.append(self._dim._repr_html_())
        r.append('</td><td>')
        r.append("".join([t._repr_html_() for t in self._tags]))
        r.append('</td>')
        return r

def tagsdim(td):
    '''Create a TagsDim field selector.

    Args:
      td (str):  Colon separated string. The dimension is the string following the final colon. All other strings are tags.

      td (list):  The dimension is the final string, preceding strings are tags.
    '''
    if type(td) in (list, tuple):
        elements = td
    elif isinstance(td, string_types):
        strd = td
        if ':' in strd:
            elements = strd.split(':')
        else:
            elements = [strd]
    else:
        raise ValueError("Expecting string or list of strings, not " + str(td))
    d = _dim(elements[-1].strip())
    tags = []
    for rawtag in elements[:-1]:
        t = rawtag.strip()
        if t:
            tag = Tag(t)
            tags.append(tag)
    return TagsDim(tags=tags, dim=d)


class Field(object):
    """ The name of a field in a data source. """
    def __init__(self, name):
        self._name = name

    def __repr__(self):
        return "Field(" + repr(self._name) + ")"

    @property
    def name(self):
        """ The name of the field """
        return self._name

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

def _field_or_tagsdim(x):
    if isinstance(x, string_types):
        if x.startswith("@"):
            return Field(x[1:])
        else:
            return tagsdim(x)
    elif isinstance(x, Field):
        return x
    elif isinstance(x, TagsDim):
        return x
    elif type(x) in (list, tuple):
        return tagsdim(x)
    else:
        raise ValueError("Expecting field, tagsdim, string or list of strings, not " + str(td))

class Dim(object):
    """ """
    def __init__(self, d):
        if not isinstance(d, string_types):
            raise ValueError("Expecting a string, not " + str(d))
        self._dim = d

    def __repr__(self):
        return "Dim(" + repr(self._dim) + ")"

    def _repr_html_(self):
        return self._dim

    def __eq__(self, other):
        return isinstance(other, Dim) and self._dim == other._dim

    def __hash__(self):
        return hash(self._dim)

def dim(d):
    if not d:
        return None
    if isinstance(d, Dim):
        return d
    if isinstance(d, string_types):
        return Dim(d)
    else:
        raise ValueError("Expecting Dim, str, or None. Not " + str(d))
_dim = dim

class Tag(object):
    """  """
    def __init__(self, t):
        if not isinstance(t, string_types):
            raise ValueError("Expecting a string, not " + str(t) + " " + str(type(t)))
        self._tag = t

    def __repr__(self):
        return "Tag(" + repr(self.name) + ")"

    def _repr_html_(self):
        return self._tag

    def __eq__(self, other):
        return isinstance(other, Tag) and self._tag == other._tag

    def __hash__(self):
        return hash(self._tag)

    @property
    def name(self):
        return self._tag

def tag(t):
    return t if isinstance(t, Tag) else Tag(t)

class TableMetadata(object):
    '''TableMetadata provides logic to map tag/dimension selectors to sets
    of fields.

    Example:

    >>> md = TableMetadata({
    ...    'field1': { 'tags' : [ 'tag1', 'tag2' ], 'dim' : 'dim1' },
    ...    'field2': { 'tags' : [ 'tag1', 'tag2' ] },
    ...    'field3': { 'dim' : 'dim1' },
    ...    'field4': { } }
    ... )
    >>> 
    '''
    def __init__(self, field_to_tagsdim):
        self._map = dict(self._from_map(field_to_tagsdim))

    def _from_map(self, m):
        for (k, v) in m.items():
            if isinstance(v, TagsDim):
                yield (k, v)
            else:
                tags = [Tag(t) for t in (v['tags'] if 'tags' in v else [])]
                dim = Dim(v['dim']) if 'dim' in v and v['dim'] else None
                yield (k, TagsDim(tags, dim))

    def _repr_html_(self):
        res = ['<table>']
        res.append('<tr><td><b>Field</b></td><td><b>Dim</b></td><td><b>Tags</b></td></tr>')
        for k in sorted(self._map.keys()):
            res.append('<tr>')
            res.extend(['<td>', k, '</td>'])
            res.extend(self._map[k]._as_trs())
            res.append('</tr>')
        res.append('</table>')
        return "".join(res)

    def _tagsdim_subset(self, x, y):
        """Return if the TagsDim `x` is compatible with `y`

        x is compatible if it has no dimension or the same dimension
        as y and if y contains a subset of the tags of x.
        """
        dims_match = not x.dim or (y.dim and set([x.dim]).issubset(set([y.dim])))
        return (x.tags.issubset(y.tags)) and (dims_match)

    def tagsdim_matches(self, tagsdim, field):
        """ Return true if `tagsdim` matches `field`."""
        if field.name not in self._map:
            return False
        metadata_td = self._map[field.name]
        return self._tagsdim_subset(tagsdim, metadata_td)

#    def fields_matching(self, tagsdim):
#        """Get the list of all fields matching `tagsdim`.
#        """
#        return [Field(f) for f, ftd in self._map.items()
#            if self.tagsdim_matches(tagsdim, Field(f))]

    def fields_matching(self, selector):
        if isinstance(selector, Field):
            return [Field(selector.name)] if selector.name in self._map else []
        elif isinstance(selector, TagsDim):
            return [Field(f) for f, ftd in sorted(self._map.items())
                    if self.tagsdim_matches(selector, Field(f))]
        else:
            raise ValueError("Expecting field or tagsdim")

    def has_field(self, f):
        return f.name in self._map

    @property
    def fields(self):
        """ Get the collection fields """
        return [Field(f) for f in self.field_names]

    @property
    def field_names(self):
        return sorted(self._map.keys())

    def __repr__(self):
        return repr(self._map)

    def save_to_file(self, filename):
        with open(filename, 'wt') as fp:
            m = {f:self._map[f].to_dict() for f in self.field_names}
            json.dump(m, fp, sort_keys=True, indent=4)

def _create_table_field_tagsdim_map(m):
    if isinstance(m, TableMetadata):
        return m
    elif isinstance(m, dict):
        return TableMetadata(m)
    else:
        raise ValueError("Expecting a dictionary, or TableMetadata, not " + str(type(m)))

class Condition(object):
    def copy(self):
        raise NotImplementedError('need to implement in subclass')
    
    @property
    def fields(self):
        return []

    def map(self, f):
        return f(self)

    def map_leaves(self, f):
        return f(self)

class TrueCondition(Condition):
    def __init__(self):
        pass

    def __eq__(self, other):
        return type(self) == type(other)

class ConstituentCondition(Condition):
    def __init__(self, parts):
        self._parts = parts

    def copy(self):
        copy = type(self)([p.copy() for p in self._parts])
        return copy

    def __repr__(self):
        return "{}({!r})".format(type(self).__name__, self._parts)

    def __eq__(self, other):
        return type(self) == type(other) and frozenset(self.parts) == frozenset(other.parts)

    @property
    def parts(self):
        return self._parts[:]

    @property
    def fields(self):
        fields = []
        for part in self._parts:
            fields.extend(part.fields)
        return fields

    def map(self, func):
        return func(type(self)([func(x) for x in self._parts]))

    def map_leaves(self, f):
        return type(self)([x.map_leaves(f) for x in self._parts])


class And(ConstituentCondition):
    pass

class Or(ConstituentCondition):
    pass

def _or_condition(parts):
    if len(parts) == 1:
        return parts[0]
    elif len(parts) > 1:
        return Or(parts)
    else:
        raise ValueError("Must have at least one condition in or")

class BinaryCondition(Condition):
    def __init__(self, lhs, rhs):
        self._lhs = lhs
        self._rhs = rhs

    def __repr__(self):
        return "{}({!r}, {!r})".format(type(self).__name__, self._lhs, self._rhs)

    def __hash__(self):
        return hash((type(self), self.lhs, self.rhs))

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.lhs == other.lhs and
                self.rhs == other.rhs)

    def copy(self):
        return type(self)(self._lhs, self._rhs)

    @property
    def lhs(self):
        return self._lhs

    @property
    def rhs(self):
        return self._rhs

    @property
    def fields(self):
        fields = []
        if isinstance(self.lhs, Field):
            fields = [self.lhs]
        return fields

class Equals(BinaryCondition):
    pass

class MatchesCond(BinaryCondition):
    pass

class GreaterThan(BinaryCondition):
    pass

class GreaterThanEqualTo(BinaryCondition):
    pass

class LessThan(BinaryCondition):
    pass

class LessThanEqualTo(BinaryCondition):
    pass

class GenericBinaryCondition(BinaryCondition):
    '''Generic binary condition, not data source specific

    Instances of BinaryCondition are created by the parsing condition
    strings. The data source converts these to more specific
    conditions

    '''
    def __init__(self, lhs, op, rhs):
        super(GenericBinaryCondition, self).__init__(lhs, rhs)
        self._op = op

    def copy(self):
        return GenericBinaryCondition(self._lhs, self._op, self._rhs)

    @property
    def op(self):
        return self._op

    def __repr__(self):
        return "GenericBinaryCondition({!r},{!r},{!r})".format(
            self.lhs, self.op, self.rhs
        )

    def __hash__(self):
        return hash((self._op, self.lhs, self.rhs)) 

    def __eq__(self, other):
        return (type(self) == type(other) and self.op == other.op
                and self.lhs == other.lhs and self.rhs == other.rhs)

class Select(object):
    '''Selection of :class:`Field` objects associated with a particular
    :class:`DataSource` possibly with match conditions for rows from
    data source

    Args:

      data_source (DataSource): Subclass of :class:`DataSource` that
        this selection is associated with.

      fields (List[:class:`Field`]): 

    '''
    def __init__(self, data_source, fields=None, condition=None, **ds_kwargs):
        fields = fields if fields else []
        condition = condition if condition else And([])

        self._data_source = data_source

        self._condition = _parse_binary_condition(condition)
        self._fields = _parse_list_fieldselectors(fields)

        self._ds_kwargs = copy.deepcopy(ds_kwargs)

    def __repr__(self):
        return "Select({!r}, {!r}, {!r}, {!r})".format(
            self._data_source, self._fields, self._condition, self._ds_kwargs
        )

    def copy(self):
        return Select(self._data_source, self._fields, self._condition,
                      **self._ds_kwargs)

    @property
    def ds_args(self):
        ''' :class:`DataSource`-specific keyword args for this selection
        '''
        return namedtuple(
            'DataSourceArgs', sorted(self._ds_kwargs.keys())
        )(**self._ds_kwargs)

    @property
    def fields(self):
        return self._fields[:]

    @property
    def condition(self):
        return self._condition.copy()

    def where(self, condition=None, **kw_args):
        if condition:
            condition = And([_parse_binary_condition(condition), self._condition])
        else:
            condition = self._condition

        new_kwargs = copy.deepcopy(self._ds_kwargs)
        new_kwargs.update(kw_args)

        select = Select(self._data_source, self.fields, condition, **new_kwargs)
        select.check()
        return select

    def with_fields(self, fields):
        return Select(self._data_source, fields, self._condition,
                      **self._ds_kwargs)

    def check(self):
        return self._data_source.check_select(self)

    def debug(self):
        return self._data_source.debug_select(self)

    def run(self):
        ''' Execute a query.

        Returns a data source specific object containing the results
        '''
        return self._data_source.run(self)


class DataSource(object):
    def __init__(self, metadata, description, op_dict):
        '''
        Args:
          metadata: Table metadata
          description: Description
          op_dict: Dictionary from infix operator name to Condition
        '''
        self.description = description if description else ""
        self._metadata = metadata
        self._op_dict = op_dict

    @property
    def name(self):
        return getattr(self, '_name') if hasattr(self, '_name') else "Unknown"

    @property
    def metadata(self):
        return self._metadata

    @property
    def all_field_names(self):
        return sorted(self._metadata.field_names)

    def __repr__(self):
        return "DataSource({})".format(repr(self.name))

    def _repr_html_(self):
        return self._metadata._repr_html_()

    def _field_names(self, select):
        field_names = set()
        for selector in select._fields:
            field_names.update(
                f.name for f in self._metadata.fields_matching(selector)
            )
        return sorted(field_names)

    def get_field_names(self, *tdims):
        '''Given tagged dimensions, return list of field names that match

        Args:
          *tdims (*str): tagged dimensions as strings (e.g. "source:ip")

        Examples:

        >>> sqldata.get_field_names('source:ip','dest:ip')
        ['src_ip', 'dst_ip']
        >>> sqldata.get_field_names('ip','host')
        ['src_ip', 'dst_ip', 'src_host', 'dst_host']

        '''
        fields = set()
        for tdim in tdims:
            fields.update(
                self._metadata.fields_matching(tagsdim(tdim))
            )
        return [f.name for f in fields]
    
    def check_select(self, select):
        '''Perform data source specific checks on the query'''
        return False

    def debug_select(self, select):
        '''Print data source specific debug output for the query'''
        return False

    def run(self, select):
        raise NotImplementedError('need to implement in subclass')

    def select(self, fields='*', condition=None, **ds_args):
        fields = _parse_list_fieldselectors(fields)
        return Select(self, fields, condition, **ds_args)

    def _rewrite_generic_binary_condition(self, cond):
        '''Replace generic binary conditions by data source specific binary conditions'''
        def rewrite(obj):
            if isinstance(obj, GenericBinaryCondition):
                if obj.op in self._op_dict:
                    return self._op_dict[obj.op](obj.lhs, obj.rhs)
                else:
                    raise ValueError("Operator [{}] not supported by {}".format(obj.op, self.name))
            else:
                return obj
        return cond.map_leaves(rewrite)

    def _rewrite_tagsdim(self, cond):
        '''Replace tagsdim with fields'''
        def rewrite(obj):
            if isinstance(obj, GenericBinaryCondition) and isinstance(obj.lhs, TagsDim):
                fields = self._metadata.fields_matching(obj.lhs)
                if not fields:
                    raise ValueError("No fields matching {}".format(repr(obj.lhs)))
                res = _or_condition([GenericBinaryCondition(f, obj.op, obj.rhs) for f in fields])
                return res
            else:
                return obj
        res = cond.map_leaves(rewrite)
        return res

    def _rewrite_outer_and(self, cond):
        '''Unnest ands. For example And(And(x,y),And(z)) -> And(x,y,z)'''
        def walk(obj):
            if isinstance(obj, And):
                for x in obj.parts:
                    for y in walk(x):
                        yield y
            else:
                yield obj
        cs = list(walk(cond))
        if cs:
            if len(cs)==1:
                return cs[0]
            else:
                return And(cs)
        else:
            return TrueCondition()

    def _check_fields(self, cond):
        not_found = []
        for f in cond.fields:
            if not self._metadata.has_field(f):
                not_found.append(f.name)
        if not_found:
            raise ValueError(
                "Fields not present in datasource {}: {}".format(self.name, str(set(not_found))))

    def _rewrite(self, cond):
        res = self._rewrite_outer_and(
            self._rewrite_generic_binary_condition(
                self._rewrite_tagsdim(cond)
            )
        )
        self._check_fields(cond)
#        print(res)
#        return self._rewrite_outer_and(
#            self._rewrite_generic_binary_condition(self._rewrite_tagsdim(cond)))
#        return self._rewrite_generic_binary_condition()
        return res

class Registry(dict):
    '''A collection of data sources.

    Args:

      data_sources (Dict[:class:`DataSource`])): dictionary from data
        source names to :class:`DataSource` objects

    Example:

    >>> registry = Registry( {
    ... })
    >>>
    '''

    def __init__(self, data_sources):
        self.update(data_sources)

    def _repr_html_(self):
        res = ['<table>']
        def td(d):
            res.extend(['<td>',d,'</td>'])
        res.append('<th>Name</th><th>Class</th><th>Description</th></tr>')
        for k in sorted(self.keys()):
            ds = self[k]
            res.append('<tr>')
            td(k)
            td(ds.__class__.__name__)
            td(ds.description)
            res.append('</tr>')
        res.append('</table>')
        return "".join(res)


################################################################################
# Parsing
################################################################################
import pyparsing
from pyparsing import srange, nums, quotedString, delimitedList
from pyparsing import Combine, Word, LineStart, LineEnd, Optional, Literal

def _rhs_p():
    Ipv4Address = Combine(Word(nums) + ('.'+Word(nums))*3).setResultsName('ipv4')
    Ipv4Address = Ipv4Address.setParseAction(lambda s, l, toks: toks[0])

    Int = Word(nums).setResultsName('int')
    Int = Int.setParseAction(lambda s, l, toks: int(toks[0]))

    Float = Combine(Word(nums) + '.' + Word(nums)).setResultsName('float')
    Float = Float.setParseAction(lambda s, l, toks: float(toks[0]))

    String = quotedString.setResultsName('string').addParseAction(pyparsing.removeQuotes)

    rhs = pyparsing.Or([String, Int, Float, Ipv4Address]).setResultsName('rhs')
    return rhs

def _tagdim_field_p():
    td = Word(srange('[-_a-zA-Z0-9:]')).setResultsName('tagsdim')
    f = Combine('@' + Word(srange('[_a-zA-Z]+'))).setResultsName('field')
    parser = (f | td).setParseAction(lambda s, l, toks: _field_or_tagsdim(toks[0]))
    return parser

def _list_tagdim_field_p():
    def p(s, l, toks):
        return toks[0]
    star = Literal('*').setResultsName('star').setParseAction(p)
    optTds = Optional(delimitedList(_tagdim_field_p())).setParseAction(lambda s, l, toks: toks)
    return star |  optTds

def _binary_condition_p():
    lhs = _tagdim_field_p().setResultsName('lhs')
    op = Word('[!=<>~]').setResultsName('op')

    rhs = _rhs_p()
    line = (LineStart() + lhs + op + rhs + LineEnd()).setResultsName('Line')

    line.setParseAction(lambda s, l, toks: GenericBinaryCondition(toks[0], toks[1], toks[2]))
    return line

def _parse_list_fieldselectors(fields):
    if isinstance(fields, (list,tuple)):
        return fields
        # return sum([_parse_list_fieldselectors(f) for f in fields], [])
    r = _list_tagdim_field_p().parseString(fields, parseAll=True).asList()
    if len(r) >= 1 and r[0] == '*':
        r = []
    return r

def _parse_binary_condition(condition):
    if isinstance(condition, Condition):
        return condition
    elif not isinstance(condition, str):
        raise ValueError(
            "Expecting string or Condition, not {}={}".format(
                type(condition),condition
            )
        )
    return _binary_condition_p().parseString(condition)[0]


