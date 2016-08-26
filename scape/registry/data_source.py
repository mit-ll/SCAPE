from __future__ import absolute_import

from .condition import (
    Or, or_condition, And, TrueCondition, GenericBinaryCondition,
)
from .parsing import parse_list_fieldselectors
from .select import Select
from .tagged_dim import TaggedDim, tagged_dim
from .field import Field, field

class DataSource(object):
    '''Model of data sources (i.e. databases, data stores) to be accessed
    by analysts

    Args:

      metadata (:class:`TableMetadata`): TableMetadata mapping tag/dimension
        selectors to sets of fields.

      description (str): Short description of data source

      op_dict (Dict[str, :class:`Condition`]): Dictionary mapping
        infix operators to Condition types

    Examples:

        >>> ds = AddcSomeDbDataSource(
        ...   metadata=TableMetadata({
        ...    'field1': { 'tags' : [ 'tag1', 'tag2' ], 'dim' : 'dim1' },
        ...    'field2': { 'tags' : [ 'tag1', 'tag2' ] },
        ...    'field3': { 'dim' : 'dim1' },
        ...    'field4': { }, 
        ...   }),
        ...   description='SomeDb storage of ADDC data',
        ...   op_dict={
        ...     '==': Equals,
        ...     '<': LessThan,
        ...     '<=': LessThanEqualTo,
        ...   }
        ... )
        >>> select = ds.select('dim1').where('tag1: == "value*with*wcards"')
        >>> rows = list(select)

    '''
    def __init__(self, metadata, description, op_dict):
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

    @property
    def tags(self):
        '''Get the list of all tag names associated with some data source field.'''
        return self._metadata.tags

    @property
    def dims(self):
        '''Get the list of all dimension names associated with some data source field.'''
        return self._metadata.dims

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
                self._metadata.fields_matching(tagged_dim(tdim))
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
        fields = parse_list_fieldselectors(fields)
        return Select(self, fields, condition, **ds_args)

    def _rewrite_generic_binary_condition(self, cond):
        '''Replace generic binary conditions by data source specific binary
        conditions
        
        '''
        def rewrite(obj):
            if isinstance(obj, GenericBinaryCondition):
                if obj.op in self._op_dict:
                    return self._op_dict[obj.op](obj.lhs, obj.rhs)
                else:
                    raise ValueError(
                        "Operator [{}] not supported by {}".format(
                            obj.op, self.name
                        )
                    )
            else:
                return obj
        return cond.map_leaves(rewrite)

    def _rewrite_tagged_dim(self, cond):
        '''Replace tagged_dim with fields'''
        def rewrite(obj):
            if ( isinstance(obj, GenericBinaryCondition) and
                 isinstance(obj.lhs, TaggedDim) ):
                fields = self._metadata.fields_matching(obj.lhs)
                if not fields:
                    raise ValueError(
                        "No fields matching {}".format(repr(obj.lhs))
                    )
                res = or_condition(
                    [GenericBinaryCondition(f, obj.op, obj.rhs)
                     for f in fields]
                )
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
                "Fields not present in datasource {}: {}".format(
                    self.name, str(set(not_found))
                )
            )

    def _rewrite(self, cond):
        res = self._rewrite_outer_and(
            self._rewrite_generic_binary_condition(
                self._rewrite_tagged_dim(cond)
            )
        )
        self._check_fields(cond)
#        print(res)
#        return self._rewrite_outer_and(
#            self._rewrite_generic_binary_condition(self._rewrite_tagged_dim(cond)))
#        return self._rewrite_generic_binary_condition()
        return res
