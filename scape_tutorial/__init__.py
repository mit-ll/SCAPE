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

__all__ = ['connection','data',]

index = None

start_text="""

Hello and welcome to the tutorial for red{SCAPE}, the Scalable Cyber
Analytic Processing Environment.

In this tutorial, we will learn a little about how the system
operates, how to ask questions about the network information it
stores, and how to create scalable analytics based on this
information.

To proceed, type f and hit return
"""

def start():
    import scape.tutorial.connection
    from scape.tutorial.text import text
    import __main__
    C = scape.tutorial.connection.TutorialConnection()
    S = C.registry.selection
    Q = S.question
    __main__.__builtins__.C = C
    __main__.__builtins__.S = S
    __main__.__builtins__.Q = Q
    __main__.__builtins__.f = f

    global index
    index = 0
    _setup_steps()
    print(text(start_text))

class _forward(object):
    def __repr__(self):
        global index
        _next()
        index += 1
        return ''

f = _forward()

_steps = None
def _setup_steps():
    import scape.tutorial.about_python
    global _steps
    _steps = [
        scape.tutorial.about_python.start,
        ]
    
def _next():
    if index < len(_steps):
        _steps[index]()
    

