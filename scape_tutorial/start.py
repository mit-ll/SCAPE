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

index = None

start_text="""

Hello and welcome to the tutorial for red(SCAPE), the red(S)calable
red(C)yber red(A)nalytic red(P)rocessing red(E)nvironment.

In this tutorial, we will learn a little about how the system
operates, how to as questions about the network information it stores,
and how to create scalable analytics based on this information. 

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

    global index
    index = 0

    print(text(start_text))

