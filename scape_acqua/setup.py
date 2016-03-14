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
import codecs  # To use a consistent encoding
import os

from setuptools import setup, find_packages  

HERE = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    # intentionally *not* adding an encoding option to open, See:
    #   https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    return codecs.open(os.path.join(HERE, *parts), 'r').read()

# Get the long description from the relevant file
with codecs.open(os.path.join(HERE, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

setup(
    name='scape',

    # Versions should comply with PEP440.  For a discussion on
    # single-sourcing the version across setup.py and the project
    # code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version=find_version('scape','__init__.py'),

    description='Scalable Cyber Analytic Processing Environment',
    long_description=long_description,

    # The project's main homepage.
    url='https://llcad-github/dogwynn/scape',

    # Author details
    author='David O\'Gwynn',
    author_email='dogwynn@ll.mit.edu',

    # Choose your license
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Databases',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular,
        # ensure that you indicate whether you support Python 2,
        # Python 3 or both.
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],

    # What does your project relate to?
    keywords='cyber analytics scalability',

    # You can just specify the packages manually here if your project
    # is simple. Or you can use find_packages().
    packages=find_packages(
        exclude=[
            'conf', 'bin', 'data','dependencies','notebooks',
            'registries','rucksack','test',
        ]
    ),
    include_package_data=True,

    # List run-time dependencies here.  These will be installed by pip
    # when your project is installed. For an analysis of
    # "install_requires" vs pip's requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    # install_requires=[
    #     'lrparsing',
    #     'GeoIP',
    #     'colorbrewer',
    #     'httplib2',
    #     'ipaddress',
    #     'kafka-python',
    #     'networkx',
    #     'psutil',
    #     'redis',
    #     'thrift',
    #     'titlecase',
    #     'urllib3',
    #     'scapy',
    # ],
 
    # List additional groups of dependencies here (e.g. development
    # dependencies).  You can install these using the following
    # syntax, for example:
    # $ pip install -e .[dev,test]
    # extras_require = {
    #     'dev': ['check-manifest'],
    #     'test': ['coverage'],
    # },

    # If there are data files included in your packages that need to
    # be installed, specify them here.  If using Python 2.6 or less,
    # then these have to be included in MANIFEST.in as well.
    # package_dir={'scape.utils.services': 
    # package_data={
    #     'scape': ['scape/utils/services/service-names-port-numbers.csv'],
    # },

    # Although 'package_data' is the preferred approach, in some case
    # you may need to place data files outside of your packages.  see
    # http://docs.python.org/3.4/distutils/setupscript.html#installing-additional-files
    # In this case, 'data_file' will be installed into
    # '<sys.prefix>/my_data'
    # data_files=[('my_data', ['data/data_file'])],

    # To provide executable scripts, use entry points in preference to
    # the "scripts" keyword. Entry points provide cross-platform
    # support and allow pip to create the appropriate form of
    # executable for the target platform.
    # entry_points={
    #     'console_scripts': [
    #         'sample=sample:main',
    #     ],
    # },
)
