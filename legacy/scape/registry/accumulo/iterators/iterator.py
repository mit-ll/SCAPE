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
#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ..proxy.ttypes import (
    IteratorSetting, 
    )


class BaseIterator(object):
    """docstring for BaseIterator"""
    classname = None

    def __init__(self,name=None,priority=10):
        self.name = None
        if name is None:
            if self.classname:
                self.name = self.classname.split('.')[-1]
        self.priority = priority

    @property
    def setting(self):
        setting = IteratorSetting()
        setting.priority = self.priority
        setting.name = self.name
        setting.iteratorClass = self.classname
        setting.properties = self.properties
        return setting

    @property
    def properties(self):
        return {}

class RowDeletingIterator(BaseIterator):
    """
    An iterator for deleting whole rows. After setting this iterator up for
    your table, to delete a row insert a row with empty column family, empty
    column qualifier, empty column visibility, and a value of DEL_ROW. Do not
    use empty columns for anything else when using this iterator. When using
    this iterator the locality group containing the row deletes will always be
    read. The locality group containing the empty column family will contain
    row deletes. Always reading this locality group can have an impact on
    performance. For example assume there are two locality groups, one
    containing large images and one containing small metadata about the images.
    If row deletes are in the same locality group as the images, then this will
    significantly slow down scans and major compactions that are only reading
    the metadata locality group. Therefore, you would want to put the empty
    column family in the locality group that contains the metadata. Another
    option is to put the empty column in its own locality group. Which is best
    depends on your data.
    """
    classname="org.apache.accumulo.core.iterators.user.RowDeletingIterator"
    def __init__(self, **kw):
        super(RowDeletingIterator, self).__init__(**kw)

