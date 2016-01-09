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
# -*- coding: utf-8 -*-

from __future__ import absolute_import
import ast
import json
import os
import sys
import glob
import logging as _logging
import traceback
from datetime import datetime
import __main__

import scape.utils

from . import logging

__all__ = ['logging']

_log = scape.utils.new_log('scape.config')

CONFIG_ENV_VAR = 'SCAPE_CONFIG'

class ScapeConfigError(Exception):
    pass

def expand_vars(config):
    stack = [config]
    while stack:
        D = stack.pop()
        for key,val in D.items():
            if type(val) in {list,tuple}:
                nval = list(val)
                for i,v in enumerate(val):
                    nval[i] = os.path.expandvars(v)
                if type(val) is tuple:
                    D[key] = tuple(nval)
                else:
                    D[key] = nval
            elif isinstance(val,dict):
                stack.append(val)
            elif type(val) in {str,unicode}:
                D[key] = os.path.expandvars(val)

def expand_registry(config):
    if 'registry' in config:
        if 'paths' in config['registry']:
            if config['registry']['paths']:
                P = config['registry']['paths'].split(os.pathsep)
                P = reduce(lambda x,y:x+y,
                           [glob.glob(p) for p in P],[])
                config['registry']['paths'] = P
            # Read in JSON objects from paths
            J = [scape.utils.read_json(p) for p in config['registry']['paths']]
            # Merge the JSON objects into a single object
            config['registry']['object'] = reduce(scape.utils.merge_dicts,J,{})
        if 'time_events' in config['registry']:
            config['registry']['time_events'] = config['registry']['time_events'].split(
                os.pathsep
            )

class Config(dict):
    def __init__(self):
        try:
            if not _logging.getLogger().handlers:
                _logging.basicConfig()
            if CONFIG_ENV_VAR in os.environ:
                config_path = os.environ[CONFIG_ENV_VAR]
                P = config_path.split(os.pathsep)
                _log.info('config paths: %s',P)
                self.from_paths(P)
            else:
                _log.error(
                    "{} environment variable not in environment, " +
                    "unable to load configuration".format(CONFIG_ENV_VAR))

            check_errors = self.check()
            if check_errors:
                raise ScapeConfigError("\n\n".join(
                    ["** {}".format(e) for e in check_errors]
                ))
        except:
            _log.error("Loading configuration failed:")
            _log.error(traceback.format_exc())

    def check(self):
        home = self.user_home
        errors = []
        if not os.access(home,os.W_OK):
            errors.append("SCAPE_USER_HOME cannot be created, parent"
                          " directory not writable.")

        return errors

    def get(self,key,default={}):
        return dict.get(self,key,default)

    def from_paths(self,paths):
        try:
            for path in paths:
                if os.path.exists(path):
                    D = scape.utils.read_json(path)
                    if not isinstance(D,dict):
                        _log.error(
                            " Configuration file doesn't contain a"
                            " valid JSON object: {}".format(path))
                    else:
                        scape.utils.merge_dicts(self,D)
                else:
                    _log.error(
                        "Configuration file doesn't exists: {}".format(path))
            expand_vars(self)
            # expand the registry
            expand_registry(self)
        except:
            _log.error("Loading configuration failed: {}".format(path))
            _log.error(traceback.format_exc())


    @property
    def user_home(self):
        home_dir = os.path.abspath(self['user_home'])
        if not os.path.exists(home_dir):
            os.makedirs(home_dir)
        return home_dir



#load the configuration file when this module loads
config = Config()

from scape.config.logging import (
    setup_logging, setup_platform_logging, setup_sge_logging,
)

from scape.config.data import (
    data_path, groups_path, splunk_raw_data_path,
    splunk_data_path, splunk_csv_paths, inbox_path,
    home_path,
)
