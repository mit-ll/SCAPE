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

import re
import abc

class _colors(abc.ABCMeta):
    END = '\033[0m'

    def __new__(cls, name, parents, dct):
        colorlut = {}
        def color_func(color):
            @classmethod
            def wrap(cls,text):
                return color + text + _colors.END
            return wrap
        for key,value in list(dct.items()):
            if isinstance(value,str):
                dct[key] = color_func(value)
                colorlut[key] = dct[key]
        dct['colors'] = colorlut
                
        return super(_colors,cls).__new__(cls, name, parents, dct)
        
class colors(object, metaclass=_colors):
    gray = '\033[90m'
    red = '\033[91m'
    green = '\033[92m'
    yellow = '\033[93m'
    blue = '\033[94m'
    magenta = '\033[95m'
    cyan = '\033[96m'
    white = '\033[97m'

    colors=None

    @classmethod
    def format(cls,text):
        colorfunc_re = re.compile(r'(?P<func>\w+){(?P<arg>.*?)}')
        def crepl(match):
            gdict = match.groupdict()
            f,a = list(map(gdict.get,['func','arg']))
            for cname,cfunc in list(cls.colors.items()):
                if cname.startswith(f):
                    return getattr(cls,cname)(a)
        return colorfunc_re.sub(crepl,text)
