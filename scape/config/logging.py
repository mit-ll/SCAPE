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
from __future__ import absolute_import
import os
import sys
import logging as _logging

from datetime import datetime
import __main__

import scape.config

from scape.utils import sge

_log = _logging.getLogger('scape.config.logging')
_log.addHandler(_logging.NullHandler())

class LevelFilter(_logging.Filter):
    def __init__(self,levels,*a,**kw):
        self._levels = levels
        _logging.Filter.__init__(self,*a,**kw)
    def filter(self,record):
        return record.levelno in (self._levels)

logging_levels = [
    _logging.DEBUG, _logging.INFO, _logging.WARNING,
    _logging.ERROR, _logging.CRITICAL, _logging.FATAL,
    ]
    
logging_level_lut = {_logging.getLevelName(l):l for l in logging_levels}
logging_level_lut['WARN'] = logging_level_lut["WARNING"]


def setup_platform_logging(log_level=None):
    log_level = log_level or scape.config.config['log_level']
    log_level = logging_level_lut[log_level.upper()]
    log_format = scape.config.config['log_format']

    handlers = []

    formatter = _logging.Formatter(log_format)

    if sge.is_sge_process():
        setup_sge_logging()
    else:
        stdout = _logging.StreamHandler(sys.stdout)
        stdout.setLevel(log_level)
        stdout.addFilter(LevelFilter([_logging.DEBUG,_logging.INFO]))
        stdout.setFormatter(formatter)

        stderr = _logging.StreamHandler(sys.stderr)
        stderr.setLevel(max(log_level,_logging.WARN))
        stderr.setFormatter(formatter)

        handlers.extend([stdout,stderr])

        if hasattr(__main__,'__file__'):
            _,script_file = os.path.split(__main__.__file__)
            log_base_dir = scape.config.config['platform_log_base_dir']
            log_base_dir = os.path.abspath(log_base_dir)
            log_dir = os.path.join(log_base_dir,os.environ['USER'],script_file)
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            log_filename = "{ts}.log".format(
                ts=timestamp,
                )
            log_path = os.path.join(log_dir,log_filename)

            logfile = _logging.FileHandler(log_path)
            logfile.setLevel(log_level)
            logfile.setFormatter(formatter)
            handlers.append(logfile)

        root = _logging.getLogger()
        root.removeHandler(root.handlers[0])

        for h in handlers:
            root.addHandler(h)

        root.setLevel(log_level)

def setup_sge_logging(level=None):
    log_level = logging_level_lut[scape.config.config['log_level'].upper()]
    log_format = scape.config.config['log_format']

    #_logging.basicConfig(level=log_level,format=log_format)

    handlers = []

    formatter = _logging.Formatter(log_format)
    stderr = _logging.StreamHandler(sys.stderr)
    stderr.setLevel(log_level)
    stderr.setFormatter(formatter)
    handlers.append(stderr)

    root = _logging.getLogger()
    root.removeHandler(root.handlers[0])

    for h in handlers:
        root.addHandler(h)

    root.setLevel(log_level)

def setup_logging(level=None):
    if sge.is_sge_process():
        setup_sge_logging(level)
    else:
        if level:
            if type(level) in (str,unicode):
                level = logging_level_lut[level.upper()]
        else:
            level = logging_level_lut[scape.config.config['log_level'].upper()]

        log_level = level
        log_format = scape.config.config['log_format']

        _logging.basicConfig(level=log_level,format=log_format)

        root = _logging.getLogger()
        if root.getEffectiveLevel() != log_level:
            root.setLevel(log_level)

        formatter = _logging.Formatter(log_format)
        for h in root.handlers:
            h.setFormatter(formatter)
            h.setLevel(log_level)
    
