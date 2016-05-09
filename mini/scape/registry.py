"""Scape Registry."""
import copy
import re
import json
from six import string_types # python pos

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
        return {'tags' : [t.name for t in self.tags], 'dim' : self.dim}

    def _as_trs(self):
        r = ['<td>']
        if self._dim:
            r.append(self._dim._repr_html_())
        r.append('</td><td>')
        r.append("".join([t._repr_html_() for t in self._tags]))
        r.append('</td>')
        return r

def td(dim=None, *tags):
    tags = [tag(t) for t in tags] if tags else []
    return TagsDim(tags, _dim(dim))


def tagsdim(td):
    if type(td) in (list, tuple):
        elements = td
    elif isinstance(td, str):
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
        return "Field(" + self._name + ")"

    @property
    def name(self):
        """ The name of the field """
        return self._name

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

def _field_or_tagsdim(x):
    if isinstance(x, str):
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
        return "Dim(" + self._dim + ")"

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
    if isinstance(d, str):
        return Dim(d)
    else:
        raise ValueError("Expecting Dim, str, or None. Not " + str(d))

def _dim(d):
    """An alias to dim(), allowing functions to call dim() while have a
    parameters or variables named dim."""
    return dim(d)

class Tag(object):
    """  """
    def __init__(self, t):
        if not isinstance(t, string_types):
            raise ValueError("Expecting a string, not " + str(t) + " " + str(type(t)))
        self._tag = t

    def __repr__(self):
        return "Tag('" + self._tag + "')"

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

class Registry(object):
    """ A collection  of data sources. """

    def __init__(self, data_sources):
        """A dictionary from data source names to data source."""
        self._data_sources = data_sources

    def data_source(self, name):
        return self._data_sources[name]

    def data_sources(self):
        return self._data_sources.values()

    def __getitem__(self, name):
        return self._data_sources[name]

    def items(self):
        return self._data_sources.items()



class TableMetadata(object):
    """ TableMetadata provides logic to map tag/dimension selectors to sets of fields. """
    def _from_map(self, m):
        for (k, v) in m.items():
            if isinstance(v, TagsDim):
                yield (k, v)
            else:
                tags = [Tag(t) for t in (v['tags'] if 'tags' in v else [])]
                dim = Dim(v['dim']) if 'dim' in v and v['dim'] else None
                yield (k, TagsDim(tags, dim))

    def __init__(self, m):
        """ Construct table metadata from a dictionary of tags and dimensions

        Example:
            TableMetadata({
               'field1': { 'tags' : [ 'tag1', 'tag2' ], 'dim' : 'dim1' },
               'field2': { 'tags' : [ 'tag1', 'tag2' ] },
               'field3': { 'dim' : 'dim1' },
               'field4': { } })
        """
        self._map = dict(self._from_map(m))

    def _repr_html_(self):
        res = ['<table>']
        res.append('<tr><td>Field</td><td>Dim</td><td>Tags</td></tr>')
        for k in sorted(self._map.keys()):
            res.append('<tr>')
            res.append('<td>')
            res.append(k)
            res.append('</td>')
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
        """ Return true if `tagsdim` matches `field`.

        TODO: Should this raise an exception on fields not included in the table metadata?
        """
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
            return [Field(f) for f, ftd in self._map.items()
                    if self.tagsdim_matches(selector, Field(f))]
        else:
            raise ValueError("Expecting field or tagsdim")

    def has_field(self, f):
        return f.name in self._map

    @property
    def fields(self):
        """ Get the collection fields """
        return [Field(f) for f in self._map.keys()]

    @property
    def field_names(self):
        return self._map.keys()

    def __repr__(self):
        return str(self._map)

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
    def fields(self):
        return []

    def map(self, f):
        return f(self)

    def map_leaves(self, f):
        return f(self)

class TrueCondition(Condition):
    def __init__(self):
        pass

class And(Condition):
    def __init__(self, xs):
        self.xs = xs

    def fields(self):
        for c in self.xs:
            for f in c.fields():
                yield f

    def map(self, f):
        def g(x):
            print(x, "------->", f(x))
            return f(x)
        return f(And([g(x) for x in self.xs]))

    def map_leaves(self, f):
        return And([x.map_leaves(f) for x in self.xs])

    def __repr__(self):
        return "And({})".format(str(repr(self.xs)))

class BinaryCondition(Condition):
    def __init__(self, lhs, rhs):
        self._lhs = lhs
        self._rhs = rhs

    def __repr__(self):
        return "BinaryCondition({},{})".format(self._lhs, self._rhs)

    @property
    def lhs(self):
        return self._lhs

    @property
    def rhs(self):
        return self._rhs

    def fields(self):
        if isinstance(self.lhs, Field):
            yield self.lhs

class GenericBinaryCondition(BinaryCondition):
    """ Generic binary condition, not data source specific""

    Instances of BinaryCondition are created by the parsing condition
    strings. The data source converts these to more specific conditions"""
    def __init__(self, lhs, op, rhs):
        super(GenericBinaryCondition, self).__init__(lhs, rhs)
        self._op = op

    @property
    def op(self):
        return self._op

    def __repr__(self):
        return "{} {} {}".format(repr(self.lhs), repr(self.op), repr(self.rhs))

    def __eq__(self, other):
        return (type(self) == type(other) and self.op == other.op
                and self.lhs == other.lhs and self.rhs == other.rhs)


class Equals(BinaryCondition):
    def __init__(self, lhs, value):
        super(Equals, self).__init__(lhs, value)

    def __repr__(self):
        return "{} == {}".format(repr(self.lhs), repr(self.rhs))

    def __eq__(self, other):
        return type(self) == type(other) and self.lhs == other.lhs and self.rhs == other.rhs

class MatchesCond(BinaryCondition):
    def __init__(self, lhs, value):
        super(MatchesCond, self).__init__(lhs, value)

    def __repr__(self):
        return "{} =~ {}".format(repr(self.lhs), repr(self.rhs))

class GreaterThan(BinaryCondition):
    def __init__(self, lhs, value):
        super(GreaterThan, self).__init__(lhs, value)

    def __repr__(self):
        return "{} > {}".format(repr(self.lhs), repr(self.rhs))

class GreaterThanEqualTo(BinaryCondition):
    def __init__(self, lhs, value):
        super(GreaterThanEqualTo, self).__init__(lhs, value)

    def __repr__(self):
        return "{} >= {}".format(repr(self.lhs), repr(self.rhs))

class Or(Condition):
    def __init__(self, xs):
        self.xs = xs

    def fields(self):
        for c in self.xs:
            for f in c.fields():
                yield f

    def map(self, f):
        return f(Or([f(x) for x in self.xs]))

    def map_leaves(self, f):
        return Or([x.map_leaves(f) for x in self.xs])

    def __repr__(self):
        return "Or({})".format(repr(self.xs))

def _or_condition(xs):
    if len(xs) == 1:
        return xs[0]
    elif len(xs) > 1:
        Or(xs)
    else:
        raise ValueError("Must have at least one condition in or")


class Select(object):
    def __init__(self, data_source, fields=None, condition=And([]), **kwargs):
        """From a data_source select [fields] from rows mathcing the given condition"""
        if fields is None:
            fields = []
        self._data_source = data_source
        self._condition = condition
        self._fields = fields
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return "Select({!r}, {!r}, {!r})".format(
            self._data_source, self._fields, self._condition)

    def _copy(self):
        c = Select(self._data_source, self._fields, copy.copy(self._condition))
#        c.__dict__ = copy.deepcopy(self.__dict__)
        for k, v in self.__dict__.items():
            c.__dict__[k] = v
        return c

    def where(self, x=None):
        res = self._copy()
        if isinstance(x, str):
            newcond = _parse_binary_condition(x)
        elif isinstance(x, Condition):
            newcond = x
        else:
            raise ValueError("Expecting string or Condition, not {}".format(x))

        res._condition = And([newcond, res._condition])
        self._data_source.check_query(self)
        return res

    def fields(self, _fields):
        res = self._copy()
        res._fields = _parse_list_fieldselectors(_fields)
        return res

    def check(self):
        return self._data_source.check_select(self)

#    def field_equals(self, field, value):
#    def tagsdim_equals(self, field, value):

    def run(self):
        """ Execute a query.

        Returns a data source specific object containing the results
        """
        return self._data_source.run(self)


class DataSource(object):
#    def __dir__(self):
#        print('__dir__')
#        return dir(type(self)) + list(self.__dict__) + list(self._metadata.field_names)

    def __init__(self, metadata, description, op_dict):
        """
        Args:
          metadata: Table metadata
          description: Description
          op_dict: Dictionary from infix operator name to Condition
        """
        self._description = description if description else ""
        self._metadata = metadata
        self._op_dict = op_dict

    @property
    def name(self):
        return getattr(self, '_name') if hasattr(self, '_name') else "Unknown"

    @property
    def description(self):
        return self._description

    @property
    def metadata(self):
        return self._metadata

    def __repr__(self):
        return "DataSource({})".format(repr(self.name))

    def _repr_html_(self):
        return self._metadata._repr_html_()

    def check_select(self, select):
        pass

    def check_query(self, query):
        """Perform data source specific checks on the query"""
        pass

    def run(self, select):
        self.check_query(self)
#        print(self._data_source)
#        print(self._fields_or_tagsdim)
#        print(self._condition)
#        cond = self._rewrite(self._condition)
        raise ValueError("Implement in subclass")

    def select(self, fields='*', **kwargs):
        fields = _parse_list_fieldselectors(fields)
        return Select(self, fields, **kwargs)

    def _rewrite_generic_binary_condition(self, cond):
        """Replace generic binary conditions by data source specific binary conditions"""
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
        """Replace tagsdim with fields"""
        def rewrite(obj):
            if not obj:
                raise ValueError("Illegal Arg")
            if isinstance(obj, GenericBinaryCondition) and isinstance(obj.lhs, TagsDim):
                fields = self._metadata.fields_matching(obj.lhs)
                if not fields:
                    raise ValueError("No fields matching {}".format(repr(obj.lhs)))
                return _or_condition([GenericBinaryCondition(f, obj.op, obj.rhs) for f in fields])
            else:
                return obj
        res = cond.map_leaves(rewrite)
        return res

    def _rewrite_outer_and(self, cond):
        """Unnest ands. For example And(And(x,y),And(z)) -> And(x,y,z)"""
        def walk(obj):
            if isinstance(obj, And):
                for x in obj.xs:
                    for y in walk(x):
                        yield y
            else:
                yield obj
        return And(list(walk(cond)))

    def _check_fields(self, cond):
        not_found = []
        for f in cond.fields():
            if not self._metadata.has_field(f):
                not_found.append(f.name)
        if not_found:
            raise ValueError(
                "Fields not present in datasource {}: {}".format(self.name, str(set(not_found))))

    def _rewrite(self, cond):
        res = self._rewrite_tagsdim(cond)
        res = self._rewrite_generic_binary_condition(res)
        res = self._rewrite_outer_and(res)
        self._check_fields(cond)
#        print(res)
#        return self._rewrite_outer_and(
#            self._rewrite_generic_binary_condition(self._rewrite_tagsdim(cond)))
#        return self._rewrite_generic_binary_condition()
        return res


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

def _parse_list_fieldselectors(x):
    r = _list_tagdim_field_p().parseString(x, parseAll=True).asList()
    if len(r) >= 1 and r[0] == '*':
        r = []
    return r

def _parse_binary_condition(x):
    return _binary_condition_p().parseString(x)[0]
