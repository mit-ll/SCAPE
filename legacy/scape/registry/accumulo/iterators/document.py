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
from .iterator import BaseIterator

class IntersectingIterator(BaseIterator):
    """docstring for IntersectingIterator"""
    classname="org.apache.accumulo.core.iterators.user.IntersectingIterator"
    def __init__(self, terms, not_flags=None, **kw):
        super(IntersectingIterator, self).__init__(**kw)

        self.terms = terms
        self.not_flags = not_flags

    @property
    def properties(self):
        def encode_columns(cols):
            return "".join([ col.encode("base64") for col in cols ]).rstrip()

        props = {
            "columnFamilies": encode_columns(self.terms)
        }

        def convert_flag(flag):
            if flag == 0:
                return "\0"
            elif flag == 1:
                return "\001"
            else:
                raise Exception("invalid flag")

        def encode_not_flags(flags):
            if flags:
                return "".join( [convert_flag(flag)
                                 for flag in flags] ).encode("base64")
            else:
                return None

        if self.not_flags:
            props["notFlag"] = encode_not_flags(self.not_flags)

        return props


class IndexedDocIterator(IntersectingIterator):
    """docstring for IndexedDocIterator"""
    classname="org.apache.accumulo.core.iterators.user.IndexedDocIterator"
    def __init__(self, index_colf="i", doc_colf="e", **kw):
        super(IndexedDocIterator, self).__init__(**kw)

        self.index_colf = index_colf
        self.doc_colf = doc_colf

    @property
    def iterator_properties(self):
        props = super(IndexedDocIterator, self).properties
        props["indexFamily"] = self.index_colf
        props["docFamily"] = self.doc_colf
        return props

