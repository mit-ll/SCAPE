# Copyright (2016) Massachusetts Institute of Technology.
# Reproduction/Use of all or any part of this material shall
# acknowledge the MIT Lincoln Laboratory as the source under the
# sponsorship of the US Air Force Contract No. FA8721-05-C-0002.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

from __future__ import absolute_import

from six import string_types

from .tag import Tag, tag
from .dim import Dim, dim

class TaggedDim(object):
    '''A field selector containing any number of tags and an optional
    dimension.

    Args:

      tags (List[:class:`Tag`]): list of :class:`Tag` objects for a particular
        field

      dim (:class:`Dim`): single :class:`Dim` object for a particular field

    '''
    def __init__(self, tags=None, dim=None):
        if tags is None:
            tags = []
        for t in tags:
            if not isinstance(t, Tag):
                raise ValueError(
                    "Expecting a Tag, not {} of type {}".format(t, type(t))
                )
        if dim and not isinstance(dim, Dim):
            raise ValueError(
                "Expecting a Dim, not {} of type {}".format(dim, type(dim))
            )
        self._tags = frozenset(tags)
        self._dim = dim

    def __repr__(self):
        dstr = self._dim.__repr__() if self._dim else "None"
        return "TaggedDim({}, {})".format(repr(self._tags), dstr)

    @property
    def tags(self):
        '''List of :class:`Tag` objects assocated with this TaggedDim'''
        return self._tags

    @property
    def dim(self):
        '''The dimension or None'''
        return self._dim

    def __eq__(self, other):
        return (
            (type(self) == type(other)) and
            (self.tags == other.tags) and
            (self.dim == other.dim)
        )

    def __hash__(self):
        return hash((self.dim, self.tags))

    def to_dict(self):
        """ Convert a TaggedDim() to a dict. """
        return {'tags' : [t.name for t in self.tags], 'dim' : self.dim._dim if self.dim else None }

    def _as_trs(self):
        r = ['<td>']
        if self._dim:
            r.append(self._dim._repr_html_())
        r.append('</td><td>')
        r.append(",".join([t._repr_html_() for t in self._tags]))
        r.append('</td>')
        return r
TagsDim = TaggedDim

# def td(dim=None, *tags):
#     '''Given a string dimension and a series of string tags, return a
#     :class:`TaggedDim` object
# 
#     Args:
# 
#       dim (str): 
# 
#     '''
#     tags = [tag(t) for t in tags] if tags else []
#     return TaggedDim(tags, dim(dim))


def tagged_dim(tags_and_dim):
    '''Given either a string or a list of strings representing a series of
    tags and a dimension, return a :class:`TaggedDim` object

    Args:

      tags_and_dim (Union[str,List[str]]): either a string or a list
        of strings encoding a sequence of (optional) tags and an
        (optional) dimension

    If given as a string, it should be in the form:

    - ``"dim"`` (no tags and one dimension)
    - ``"tag1:tag2:...:tagN:dim"`` (N tags and one dimension) or
    - ``"tag1:tag2:...:tagN:"`` (N tags and no dimension)

    '''
    if type(tags_and_dim) in (list, tuple):
        elements = tags_and_dim
    elif isinstance(tags_and_dim, string_types):
        strd = tags_and_dim
        if ':' in strd:
            elements = strd.split(':')
        else:
            elements = [strd]
    else:
        raise ValueError(
            "Expecting string or list of strings, not {}"
            " of type {}".format(tags_and_dim,type(tags_and_dim))
        )
    d = dim(elements[-1].strip())
    tags = []
    for rawtag in elements[:-1]:
        t = rawtag.strip()
        if t:
            tag = Tag(t)
            tags.append(tag)
    return TaggedDim(tags=tags, dim=d)

tagsdim = tagged_dim
