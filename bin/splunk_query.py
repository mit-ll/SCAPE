#! /usr/bin/env python2.7
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

from __future__ import print_function
import os
import sys
from urllib2 import urlparse
import time
import re
import time
from datetime import datetime, timedelta
import json
import csv
import StringIO
import argparse
import pprint
import logging

import scape.utils
import scape.config
from scape.utils import lines
from scape.utils.args import date_convert, date_convert_help
import scape.utils.hdfs as hdfs
import scape.utils.splunk as splunk

#_log = logging.get

def fields_convert(o):
    if type(o) is str:
        return o.split(',')
    elif type(o) in (list,tuple):
        return o
def size_convert(v):
    return int(eval(v))

def last_splunk_time():
    scon = splunk.SplunkConnection()
    return scon.last_time()


def getargs():
    parser = argparse.ArgumentParser(
        description=("Splunk interface. Must either give indexes,"
                     " --start --end times (+ optional: format,fields,"
                     "directory,name,file-size,etc.) OR --input input"
                     " dictionary OR -E/--explicit-query OR --print-indexes"),
        )
    
    parser.add_argument('indexes',nargs="*")
    parser.add_argument('-f','--format',help=("Output format "),
                        choices=['csv','json','xml','raw'],
                        default='csv')
    parser.add_argument('-F','--fields',help=("Splunk fields to extract"
                                              " for given index"),
                        type=fields_convert)
    parser.add_argument('--makedirs',action='store_true',
                        help=('If the directory of the path specified'
                              ' in --output-path does not exist, create it.'))
    parser.add_argument('--file-size',type=size_convert,default=0,
                        help=('Try to output files no larger than this size'))
    parser.add_argument('-q','--quiet',
                        help=("Do not print debugging info"),
                        action='store_true')
    parser.add_argument('-s','--start',
                        help=("Starting time for query: "+
                              date_convert_help()+" If start is given as"
                              " a relative time, then it is interpreted"
                              " as a delta backwards from the ending time"
                              " (which if end is not given, then it is"
                              " assumed to be the current time."),
                        type=date_convert)
    parser.add_argument('-e','--end',
                        help=("Ending time for query: "+
                              date_convert_help()+" If end is given as"
                              " a relative time, then it is interpreted"
                              " as a delta forwards from the starting time"),
                        type=date_convert)
    parser.add_argument('--nrows',type=int,
                        help=("Only pull newest [nrows] rows from splunk"),
                        )
    parser.add_argument('--stdout',help="Output data to stdout",
                        action='store_true')
    parser.add_argument('-d','--directory',help="Output directory path")
    parser.add_argument('-o','--output-path',help="Absolute path to output file")
    parser.add_argument('-n','--filename',help="Output file name")
    parser.add_argument('-i','--input',help="""Input dictionary file path. Dictionaries should be in the form:
       {'index': <splunk_index>,
        'start': <start_time>,
        'end': <end_time>,
        'output_mode': <output_mode|default='csv'>,
         'output_path': <path_to_output_data|default='[index]_Ymd-HMS.[mode]'>,
        'fields': 'field0,field1,field2,...' [default: *]
       }""")
    parser.add_argument('-m','--output-metadata',
                        help=('Path to put output metadata. This is data'
                              ' about what was outputted by the'
                              ' query: files, formats.'
                              ' Output is in JSON form.'),
                        )
    parser.add_argument('-E','--explicit-query',
                        help=("Explicitly definied query (as opposed"
                              " to one constructed from the"
                              " other command-line inputs)."))
    parser.add_argument('--print-indexes',action='store_true')


    # --------------------------------------------------
    # [Internal to SCAPE platform]:
    # 
    # Which search head to use 
    parser.add_argument('--head',
                        help=argparse.SUPPRESS,
                        )
    # Is this splunk query part of the platform?
    parser.add_argument('--platform',action='store_true',
                        help=argparse.SUPPRESS,
                        )

    args = parser.parse_args()

    if not (args.indexes or args.input or
            args.print_indexes or args.explicit_query):
        parser.print_help()
        sys.exit(1)
    
    start,end = args.start,args.end
    if (type(start) is datetime) and (type(end) is timedelta):
        args.end = args.start + args.end
    elif (type(end) is datetime) and (type(start) is timedelta):
        args.start = args.end - args.start
    elif (end is None) and (type(start) is timedelta):
        #args.end = datetime.now()
        #args.end = last_splunk_time()
        #args.start = args.end - args.start
        args.start = '-{s}s@s'.format(s=int(args.start.total_seconds()))
        
    return args


DATA_DIR = scape.config.config['data_path']
SLEEP_FLAG_PATH = os.path.join(DATA_DIR,'sleep')
def main():
    args = getargs()

    if args.indexes or args.input or args.explicit_query:
        scape.config.logging.setup_logging()

        secs = 1
        while os.path.exists(SLEEP_FLAG_PATH):
            logging.info('Sleeping %s seconds...')
            time.sleep(secs)
            secs = sec*2 if sec < 16 else 30

        splunk_head = args.head
        logging.info('Connecting to splunk head: %s',splunk_head)
        scon = splunk.SplunkConnection(host=splunk_head)
        dargs = vars(args)
        outmeta = {'files':[],'formats':[]}
        input_dicts = []
        if args.indexes:
            for index in args.indexes:
                d = dargs.copy()
                d['index'] = index
                if args.nrows is not None:
                    d['head'] = args.nrows
                input_dicts.append(d)
        elif args.explicit_query:
            d = dargs.copy()
            input_dicts.append(d)
        elif args.input:
            d = dargs.copy()
            with open(args.input) as rfp:
                d.update(eval(rfp.read()))
            d['start'] = date_convert(d['start'])
            d['end'] = date_convert(d['end'])
            if 'fields' in d:
                d['fields'] = fields_convert(d['fields'])
            if 'nrows' in d:
                d['head'] = d['nrows']
            input_dicts.append(d)
        for idict in input_dicts:
            if idict['output_path']:
                output_path = idict['output_path']
            else:
                output_path = idict['directory']
                if not output_path:
                    output_path = 'file://{path}'.format(
                        path=os.path.abspath('.'))
                else:
                    output_path = os.path.abspath(output_path)
            url = urlparse.urlparse(output_path)
            if not url.scheme:
                output_path = 'file://{path}'.format(path=output_path)
                url = urlparse.urlparse(output_path)

            results = scon.query(**idict)
            for result in results:
                if url.scheme == 'file':
                    path = url.path
                    if idict['makedirs']:
                        if not os.path.exists(path):
                            os.makedirs(path)
                    if idict['output_path']:
                        fpath = path
                    elif idict.get('filename'):
                        fpath = os.path.join(path,idict['filename'])
                    else:
                        fpath = os.path.join(path,result['name'])
                    logging.info('\nwrote path: %s',fpath)
                    outmeta['files'].append(fpath)
                    outmeta['formats'].append('csv')
                    with open(fpath,'w') as wfp:
                        wfp.write(result['content'])
                elif url.scheme == 'hdfs':
                    outurl = urlparse.urlunparse(url)
                    fpath = os.path.join(outurl,result['name'])
                    outmeta['files'].append(fpath)
                    hdfs.putData(fpath,result['content'])
            if idict.get('output_metadata'):
                with open(idict['output_metadata'],'wb') as wfp:
                    json.dump(outmeta,wfp,sort_keys=True,indent=1)
                    
    elif args.print_indexes:
        head = args.head 
        scon = splunk.SplunkConnection(host=head)
        print('\n'.join(scon.indexes()))
        

if __name__=='__main__':
    main()
        
