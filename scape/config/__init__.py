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

'''Configuration management for Scape

The Scape environment is configured through a combination of
enrionment variables and a JSON file which maps these environment
variables to actual system settings. This module handles the loading
of these variables into a Config dictionary.

The primary environment variable for Scape is SCAPE_CONFIG. This
should be a path pointing to the JSON file from which the environment
is configured.

'''

from __future__ import absolute_import
import ast
import json
import os
import sys
import glob
import logging as _logging
import traceback
from datetime import datetime


__all__ = ['logging', 'data']


_log = scape.utils.new_log('scape.config')

from scape.config.config import (
    Config, default_config
)


from scape.config.logging import (
    setup_logging, setup_platform_logging, setup_sge_logging,
)

from scape.config.data import (
    data_path, groups_path, splunk_raw_data_path,
    splunk_data_path, splunk_csv_paths, inbox_path,
    home_path,
)
