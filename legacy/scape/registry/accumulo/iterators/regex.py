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

class RegExFilter(BaseIterator):
    """docstring for RegExFilter"""
    classname="org.apache.accumulo.core.iterators.user.RegExFilter"
    def __init__(self, row_regex=None, cf_regex=None, cq_regex=None,
                 val_regex=None, or_fields=False,
                 match_substring=False, **kw):
        super(RegExFilter, self).__init__(**kw)
        self.row_regex = row_regex
        self.cf_regex = cf_regex
        self.cq_regex = cq_regex
        self.val_regex = val_regex
        self.or_fields = or_fields
        self.match_substring = match_substring

    @property
    def properties(self):
        props = {}

        if self.row_regex: props["rowRegex"] = self.row_regex
        if self.cf_regex: props["colfRegex"] = self.cf_regex
        if self.cq_regex: props["colqRegex"] = self.cq_regex
        if self.val_regex: props["valueRegex"] = self.val_regex

        props["orFields"] = str(self.or_fields).lower()
        props["matchSubstring"] = str(self.match_substring).lower()

        return props

