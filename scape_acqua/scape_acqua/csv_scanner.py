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

'''Wrapper for Acqua CSVScanner OperatorSource

'''
import acqua
import acqua.utils

class CSVScanner(object):
    def __init__(self, files, fields=None):
        self.files = files
        self.fields = fields or []

    _operator_source = None
    @property
    def operator_source(self):
        if self._operator_source is None:
            files = acqua.utils.string_list(self.files)
            fields = acqua.utils.string_list(self.fields)
            self._operator_source = acqua.CSVScanner(files,fields)
        return self._operator_source

    @property
    def operator(self):
        return acqua.Operator.createOperator(self.operator_source)

        
            
