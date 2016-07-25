from __future__ import absolute_import

from six import string_types

class Tag(object):
    '''Semantic descriptor for fields

    Tags are used in combination with dimensions (domain-specific data
    types) to dereference fields (data-source-specific element
    names). Tags are used to describe the nature and purpose of the
    domain-specifc type stored in the field.

    E.g. The field ``src_ip`` might have the domain-specific data type
    (i.e. dimension) of ``ip``, but equally important to an analyst is
    that it represents the source of some network communication. That
    semantic aspect of this field would connect it meaningfully to
    other fields like ``src_port`` or ``src_AD_domain``.

    So in addition to giving ``src_ip`` the dimension of ``ip``, we
    could give it the tag ``source``. Thus, the analyst can look for
    unique tuples of data associated with the tag ``source`` and the
    above-mentioned fields would be provided.

    '''
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
    '''Type routing/normalizing for tag objects

    Args:

      t (Union[ ``str`` , :class:`Tag` ]): tag information

    Returns:

      Tag: If ``str`` is given, a :class:`Tag` is created. If a
        :class:`Tag` is given, it is returned directly.

    '''
    return t if isinstance(t, Tag) else Tag(t)

