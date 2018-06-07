from __future__ import absolute_import

from six import string_types

class Field(object):
    '''The  name of a field/column/cell in a data source.

    Fields represent the data-source-specific name for a particular
    individual element of data. Examples are columns names in SQL
    stores, cell names in NoSQL stores, etc.

    In Scape, they are associated (via the :class:`TableMetadata`
    object) with tags (semantic descriptors) and dimensions
    (domain-specific data types). The goal being to allow analysts to
    pose questions in terms of these tags and dimensions and have
    those questions be transformed automatically into well-formed data
    source queries.

    '''
    def __init__(self, name):
        self._name = name

    def __repr__(self):
        return "Field(" + repr(self._name) + ")"

    @property
    def name(self):
        ''' The name of the field '''
        return self._name

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

def field(f):
    ''' Type routing function for field information
    '''
    if f and isinstance(f, string_types):
        return Field(f.replace('@',''))
    elif isinstance(f, Field):
        return f
    else:
        raise ValueError(
            "Expecting str or Field object"
            " not {} of type {}".format(f,type(f))
        )
