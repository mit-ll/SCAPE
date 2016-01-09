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

def event_op(connection):
    import scape.registry.event
    def con_wrap(function):
        def wrapper(row):
            event = scape.registry.event.Event.unpickle(row,connection)
            return function(event)
        return wrapper
    return con_wrap

def load_events(info):
    from scape.registry.connection import (
        ScapeConnection
    )
    start,end,connection,selection,question = info
    if load_events.connection is None:
        load_events.connection = ScapeConnection.unpickle(*connection)
    C = load_events.connection
    S = C.registry.selection.from_node_set(selection)
    Q = S.question(question,start=start,end=end)
    return [event.pickle() for event in Q]
load_events.connection = None
    
