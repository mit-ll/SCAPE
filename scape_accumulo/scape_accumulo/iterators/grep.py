# Copyright (2016) Massachusetts Institute of Technology.  Reproduction/Use 
# of all or any part of this material shall acknowledge the MIT Lincoln 
# Laboratory as the source under the sponsorship of the US Air Force 
# Contract No. FA8721-05-C-0002.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .iterator import BaseIterator

class GrepIterator(BaseIterator):
    """docstring for GrepIterator"""
    classname="org.apache.accumulo.core.iterators.user.GrepIterator"
    def __init__(self, term, negate=False, **kw):
        super(GrepIterator, self).__init__(**kw)
        self.term = term
        self.negate = negate

    @property
    def properties(self):
        return {
            "term": self.term,
            "negate": str(self.negate).lower()
        }

