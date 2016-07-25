from __future__ import absolute_import

from six import string_types

class Dim(object):
    '''Domain-specific data types for fields

    Dimensions are used in combination with tags (semantic
    descriptors) to dereference fields (data-source specific column
    names). Dimensions are similar in a sense to more traditional data
    types (e.g. string, integer, float, etc.), but they are
    domain-specifc and are primarily concerned with denoting relevance
    to an analyst's frame of reference.

    E.g. The field ``src_ip`` might be stored in a database as a
    string (dotted quad) or as an integer (INET), but to an analyst,
    it is an `IP` address. Furthermore, it represents the `source`
    location of some network communication. So, in Scape, we might
    give this field the dimension ``ip`` and the tag ``source``.

    '''
    def __init__(self, d):
        if not isinstance(d, string_types):
            raise ValueError(
                "Expecting a string, not {} of type {}".format(d,type(d))
            )
        self._dim = d

    def __repr__(self):
        return "Dim({})".format(repr(self._dim))

    def _repr_html_(self):
        return self._dim

    def __eq__(self, other):
        return isinstance(other, Dim) and self._dim == other._dim

    def __hash__(self):
        return hash(self._dim)

    @property
    def dim(self):
        ''' The dimension name '''
        return self._dim

def dim(d):
    '''Type routing/normalizing function for dimension objects

    Args:

      d (Union[ ``None`` , :class:`Dim` , ``str`` ]): dimension information
        to return as either ``None`` or as a :class:`Dim` object

    Returns:

      None or ``Dim``: If ``None`` is provided. If a ``str`` is given, a
        :class:`Dim` is returned. If a :class:`Dim` is provided, it is
        returned directly.

    '''
    if not d:
        return None
    if isinstance(d, Dim):
        return d
    if isinstance(d, string_types):
        return Dim(d)
    else:
        raise ValueError("Expecting Dim, str, or None. Not " + str(d))

