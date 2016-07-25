from __future__ import absolute_import

from six import string_types

from .field import Field, field
from .tag import Tag, tag
from .dim import Dim, dim
from .tagged_dim import TaggedDim
from .utils import field_or_tagged_dim

class TableMetadata(object):
    '''TableMetadata provides logic to map tag/dimension selectors to sets
    of fields.

    Args:

      field_to_tagged_dim( Dict[str,Dict[str,Union[str,List[str]]]] ):
        dictionary of field name to tag-and-dimention dictionary

    Example:

        >>> md = TableMetadata({
        ...    'field1': { 'tags' : [ 'tag1', 'tag2' ], 'dim' : 'dim1' },
        ...    'field2': { 'tags' : [ 'tag1', 'tag2' ] },
        ...    'field3': { 'dim' : 'dim1' },
        ...    'field4': { } }
        ... )
        >>> md.fields_matching('tag1:')
        [Field('field1'), Field('field2')]
        >>> md.fields_matching('dim1')
        [Field('field1'), Field('field3')]

    '''
    def __init__(self, field_to_tagged_dim):
        self._map = dict(self._from_map(field_to_tagged_dim))

    def _from_map(self, m):
        for (k, v) in m.items():
            if isinstance(v, TaggedDim):
                yield (k, v)
            else:
                tags = [Tag(t) for t in (v['tags'] if 'tags' in v else [])]
                dim = Dim(v['dim']) if 'dim' in v and v['dim'] else None
                yield (k, TaggedDim(tags, dim))

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

    def _tagged_dim_subset(self, x, y):
        '''Return if the TaggedDim `x` is compatible with `y`

        x is compatible if it has no dimension or the same dimension
        as y and if y contains a subset of the tags of x.
        '''
        dims_match = not x.dim or (y.dim and set([x.dim]).issubset(set([y.dim])))
        return (x.tags.issubset(y.tags)) and (dims_match)

    def tagged_dim_matches(self, tagged_dim, field):
        '''Return true if `tagged_dim` matches `field`.

        Args:

          tagged_dim (:class:`TaggedDim`): tagged dimension object to check

          field (:class:`Field`): field to check

        TODO: Should this raise an exception on fields not included in
        the table metadata?

        '''
        if field.name not in self._map:
            return False
        metadata_td = self._map[field.name]
        return self._tagged_dim_subset(tagged_dim, metadata_td)

#    def fields_matching(self, tagged_dim):
#        '''Get the list of all fields matching `tagged_dim`.
#        '''
#        return [Field(f) for f, ftd in self._map.items()
#            if self.tagged_dim_matches(tagged_dim, Field(f))]

    def fields_matching(self, selector):
        '''Return list of :class:`Field` objects associated with a given
        selector (str, Field or :class:`TaggedDim`) if it is
        contained in this TableMetadata

        If a Field is provided, returns a list containing a
        copy of that Field.

        If a TaggedDim is provided, returns a list of
        Field objects matching the tags and dimensions in the
        TaggedDim

        Args:

          selector (:class:`Field` or :class:`TaggedDim`): selector to
            check against the TableMetadata

        Returns:

          List[:class:`Field`]: list of Field objects matching given
            selector

        '''
        if isinstance(selector, Field):
            return [Field(selector.name)] if selector.name in self._map else []
        elif isinstance(selector, TaggedDim):
            return [Field(f) for f, ftd in sorted(self._map.items())
                    if self.tagged_dim_matches(selector, Field(f))]
        elif isinstance(selector, string_types):
            selector = field_or_tagged_dim(selector)
            return self.fields_matching(selector)
        else:
            raise ValueError("Expecting Field or TaggedDim")

    def has_field(self, f):
        '''Does this TableMetadata have the given :class:`Field`

        Args:

          f (Union[ str, :class:`Field` ]): ``str`` field name or
            Field object

        Returns

          bool: whether this TableMetadata contains the given field

        '''
        return field(f).name in self._map

    @property
    def fields(self):
        '''List of :class:`Field` objects associated with this TableMetadata'''
        return [Field(f) for f in self.field_names]

    @property
    def field_names(self):
        '''List of field names (str) associated with this TableMetadata'''
        return sorted(self._map.keys())

    def __repr__(self):
        return repr(self._map)

    def save_to_json(self, filename):
        ''' Save this TableMetadata to disk as JSON
        '''
        with open(filename, 'wt') as fp:
            m = {f:self._map[f].to_dict() for f in self.field_names}
            json.dump(m, fp, sort_keys=True, indent=4)

    def save_to_yaml(self, filename):
        ''' Save this TableMetadata to disk as YAML
        '''
        m = {f:self._map[f].to_dict() for f in self.field_names}
        scape.yaml.write_yaml(m, filename)

def create_table_field_tagged_dim_map(m):
    if isinstance(m, TableMetadata):
        return m
    elif isinstance(m, dict):
        return TableMetadata(m)
    else:
        raise ValueError(
            "Expecting a dictionary, or TableMetadata,"
            " not {} of type {}".format(m, type(m))
        )
