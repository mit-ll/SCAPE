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

import os
import glob
import bisect
import re
from datetime import datetime

import geoip2.errors
import geoip2.database

import scape.config
import scape.utils.args

dt_re = re.compile(r'GeoLite2-City_(?P<Y>\d{4})(?P<m>\d{2})(?P<d>\d{2})(?P<H>\d{2})?(?P<M>\d{2})?(?P<S>\d{2})?\.mmdb')

MAXMIND_DIR = os.path.join(scape.config.config['maxmind_path'])
def geo_path(dt):
    dt = scape.utils.args.date_convert(dt)

    db_path = None

    all_db_paths = glob.glob(os.path.join(MAXMIND_DIR,'GeoLite2*City*.mmdb'))
    dated_db_paths = list(filter(dt_re.search,all_db_paths))

    if dated_db_paths:
        dated_db_paths.sort()
        dts = [datetime.strptime(os.path.split(p)[-1],'GeoLite2-City_%Y%m%d.mmdb') for p in dated_db_paths]
        index = bisect.bisect(dts,dt)
        db_path = dated_db_paths[index-1 if index else index]
    else:
        if all_db_paths:
            db_path = all_db_paths[0]

    if db_path is None:
        raise IOError('No MaxMind database to load')

    return db_path
            
    
class Geo2(object):
    def __init__(self,dt=None):
        self.cache = {}
        if dt is None:
            dt = datetime.now()
        path = geo_path(dt)
        self.reader = geoip2.database.Reader(path)
    def ip(self,ip):
        if ip in self.cache:
            return self.cache[ip]

        response = self.reader.city(ip)
        self.cache[ip] = insert_v1_keys(response.raw)
        return response.raw

def insert_v1_keys(geo):
    # area_code, city, country_code, country_code3, country_name,
    # dma_code, latitude, longitude, metro_code, postal_code, region,
    # region_name, time_zone
    geo['city_data'] = geo.get('city',{})
    v1_to_v2 = {
        'area_code': None,
        'city': ('city_data','names','en'),
        'country_code': ('country','iso_code'),
        'country_code3': ('country','iso_code'),
        'country_name': ('country','names','en'),
        'dma_code': ('location','metro_code'),
        'latitude': ('location','latitude'),
        'longitude': ('location','longitude'),
        'metro_code': ('location','metro_code'),
        'postal_code': None,
        'region': ('subdivisions','iso_code'),
        'region_name': ('subdivisions','names','en'),
        'time_zone': ('location','time_zone'),
    }
    def xpath(data,path,value=None):
        if not path:
            return value
        head,tail = path[0],path[1:]
        if isinstance(data,list):
            if len(data)>0:
                data = data[0] if isinstance(data[0],dict) else {}
            else:
                data = {}
        if head in data:
            return xpath(data[head],tail,data[head])
        return None
    for v1, v2p in list(v1_to_v2.items()):
        geo[v1] = xpath(geo,v2p)
    return geo

    

GEO2 = None
def ip2geo(ip,default=None):
    global GEO2
    if GEO2 is None:
        GEO2 = Geo2()
    try:
        geo = GEO2.ip(ip)
    except geoip2.errors.AddressNotFoundError:
        geo = default
    return geo

if 0:
    import GeoIP

    dt_re = re.compile(r'GeoLiteCity_(?P<Y>\d{4})(?P<m>\d{2})(?P<d>\d{2})(?P<H>\d{2})?(?P<M>\d{2})?(?P<S>\d{2})?\.dat')

    MAXMIND_DIR = os.path.join(scape.config.config['data_path'],'maxmind')
    def geo_path(dt):
        dt = scape.utils.args.date_convert(dt)

        db_path = None

        all_db_paths = glob.glob(os.path.join(MAXMIND_DIR,'GeoLite*City*.dat'))
        dated_db_paths = list(filter(dt_re.search,all_db_paths))

        if dated_db_paths:
            dated_db_paths.sort()
            dts = [datetime.strptime(os.path.split(p)[-1],'GeoLiteCity_%Y%m%d.dat') for p in dated_db_paths]
            index = bisect.bisect(dts,dt)
            db_path = dated_db_paths[index-1 if index else index]
        else:
            if all_db_paths:
                db_path = all_db_paths[0]

        if db_path is None:
            raise IOError('No MaxMind database to load')

        return db_path

    class Geo:
        def __init__(self,dt=None):
            if dt is None:
                dt = datetime.now()
            path = geo_path(dt)
            self.gi = GeoIP.open(
                os.path.join(path),
                GeoIP.GEOIP_MEMORY_CACHE
                )
            self.gi.set_charset(GeoIP.GEOIP_CHARSET_UTF8)
        def ip(self,ip):
            return self.gi.record_by_addr(ip)

    GEO = None
    def ip2geo(ip,default=None):
        global GEO
        if GEO is None:
            GEO = Geo()
        geo = GEO.ip(ip)
        if geo is None:
            geo = default
        return geo
