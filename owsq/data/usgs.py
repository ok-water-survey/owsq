import json
import urllib2
import StringIO
import csv
import commands
import pandas as pd
import numpy as np
#from urllib2 import urlopen
from datetime import datetime  # ,timedelta
import os
import string

from celery.task import task
from pymongo import Connection
from cybercom.data.catalog import datacommons  #catalog
from owsq import config
from owsq.util import gis_tools
from shapely.geometry import Point


username = config.catalog_username
password = config.catalog_password

mongoHost = config.mongo_host  # 'localhost'
site_database = config.usgs_database  #'ows'

#@task()
def load_site_metadata(site=None, site_collection='usgs_site', database=site_database, collection='usgs_site_metadata',
                       delete=True):
    db = Connection(mongoHost)
    if site:
        sites = site.split(',')
        for s in sites:
            get_site_metadata(s, db, database, collection)
    else:
        if delete:
            db[database][collection].remove()
        sites = db[database][site_collection].find(timeout=False)  #.limit(10)
        for site in sites:
            get_site_metadata(site['site_no'], db, database, collection)


def get_site_metadata(site, db, database, collection, ws_url=config.usgs_site_metadata_url):
    '''
        Docstring
    '''
    url = ws_url % ( site )
    f1 = urllib2.urlopen(url)
    temp = '#'
    head = ''
    while (temp[0] == "#"):
        temp = f1.readline()
        if temp[0] != '#':
            head = temp.strip('\r\n').split('\t')
    f1.readline()
    for row in f1:
        temp = row.strip('\r\n').split('\t')
        data = dict(zip(head, temp))
        try:
            param = db[database]['parameters'].find_one({'parameter_cd': data['parm_cd']})
            data['parameter'] = {'group_name': param['parameter_group_nm'], 'name': param['parameter_nm'],
                                 'units': param['parameter_units']}
        except:
            pass
        exist = db[database][collection].find_one(
            {'site_no': site, 'parm_cd': data['parm_cd'], 'srs_id': data['srs_id'],
             'data_type_cd': data['data_type_cd']})
        if exist:
            data['_id'] = exist['_id']
        db[database][collection].save(data)
        #print head


@task()
def get_metadata_site(site, ws_url=config.usgs_site_metadata_url, database=site_database, outtype='json'):
    """
         Return list of metadata from USGS webservice. Includes description of parameter, based on parm_cd
         returned from webservice. Database lookup parameter.
    """
    db = Connection(mongoHost)
    url = ws_url % (site)
    f1 = urllib2.urlopen(url)
    temp = '#'
    head = ''
    output = []
    while (temp[0] == "#"):
        temp = f1.readline()
        if temp[0] != '#':
            head = temp.strip('\r\n').split('\t')
    f1.readline()
    for row in f1:
        temp = row.strip('\r\n').split('\t')
        data = dict(zip(head, temp))
        try:
            param = db[database]['parameters'].find_one({'parameter_cd': data['parm_cd']})
            data['parameter'] = {'group_name': param['parameter_group_nm'], 'name': param['parameter_nm'],
                                 'units': param['parameter_units']}
        except:
            pass
        output.append(data)
    if outtype == 'json':
        return json.dumps(output)
    elif outtype == 'json_pretty':
        return json.dumps(output, indent=4)
    else:
        return output


@task()
def usgs_set_dbc(a, b, database='ows'):
    db = Connection(mongoHost)
    now = datetime.now()
    collection = "%s_%s" % (a, now.strftime("%Y_%m_%d_%H%M%S") )
    if a not in db[database].collection_names():
        return json.dumps({'error': 'parameter error'})
    if b not in db[database].collection_names():
        return json.dumps({'error': 'parameter error'})
    try:
        db[database][a].rename(collection)
        db[database][b].rename(a)
    except:
        raise  #pass
    return json.dumps({'Success': '%s switched with %s' % (a, b)})


@task()
def sites_usgs_update(database=site_database, collection=config.usgs_site_collection,
                      ws_url=config.usgs_site_url):  #,delete=True):
    '''**************************
        Task to update USGS 
        Sites
    '''
    db = Connection(mongoHost)
    #backup collection
    now = datetime.now()
    collection_backup = "%s_%s" % (collection, now.strftime("%Y_%m_%d_%H%M%S") )
    #try:
    #    db[database][collection].rename(collection_backup)
    #except:
    #    raise #pass
    url = ws_url
    f1_i = urllib2.urlopen(url + 'inactive')
    f2_a = urllib2.urlopen(url + 'active')
    f1_in = StringIO.StringIO(f1_i.read())
    f2_act = StringIO.StringIO(f2_a.read())
    temp = '#'
    head = ''
    while (temp[0] == "#"):
        temp = f2_act.readline()
        f1_in.readline()
        if temp[0] != '#':
            head = temp.strip('\r\n').split('\t')
    head.append('status')
    f2_act.readline()
    f1_in.readline()

    #get rtree spatial index and data object
    idx, data = gis_tools.ok_watershed_aquifer_rtree()

    #USGS Active sites
    for row in f2_act:
        temp = row.strip('\r\n').split('\t')
        temp.append('Active')
        rec = dict(zip(head, temp))
        rec['watersheds'] = []
        rec['aquifers'] = []
        try:
            row_data = rec
            #set webservices
            try:
                row_data['webservice'], row_data['parameter'], row_data['last_activity'] = get_webservice(
                    row_data['site_no'], db)
            except:
                row_data['webservice'] = []
                row_data['parameter'] = []
                row_data['last_date'] = ''

            #Check if data is available
            if len(row_data['parameter']) == 0:
                continue

            #set watershed and aquifer
            try:
                x, y = gis_tools.transform_point(rec['dec_lat_va'], rec['dec_long_va'])
                hits = list(idx.intersection((x, y, x, y)))  #, objects=True)) #[0]  #[0].object
                aPoint = Point(x, y)
                row_data = set_geo(rec, aPoint, hits, data)
            except:
                pass

            #Save site data
            db[database][collection_backup].insert(row_data)
        except:
            #Legacy code inserted without lat lon and covered errors in code
            #decided to just pass and eliminate error catching. May change back in the future
            pass
            #db[database][collection_backup].insert(rec)

    for row in f1_in:
        temp = row.strip('\r\n').split('\t')
        temp.append('Inactive')
        rec = dict(zip(head, temp))
        rec['watersheds'] = []
        rec['aquifers'] = []
        try:
            row_data = rec
            #set webservices
            try:
                row_data['webservice'], row_data['parameter'], row_data['last_activity'] = get_webservice(
                    row_data['site_no'], db)
            except:
                row_data['webservice'] = []
                row_data['parameter'] = []
                row_data['last_date'] = ''
            #Check if data is available
            if len(row_data['parameter']) == 0:
                continue
            x, y = gis_tools.transform_point(rec['dec_lat_va'], rec['dec_long_va'])
            hits = list(idx.intersection((x, y, x, y)))  #, objects=True)) #[0]  #[0].object
            aPoint = Point(x, y)
            row_data = set_geo(rec, aPoint, hits, data)

            #save site data
            db[database][collection_backup].insert(row_data)
        except:
            #Legacy code inserted without lat lon and covered errors in code
            #decided to just pass and eliminate error catching. May change back in the future
            pass
            #db[database][collection_backup].insert(rec)

    return {'source': 'usgs', 'url': [url + 'inactive', url + 'active'], 'database': database,
            'collection': collection_backup, 'record_count': db[database][collection_backup].count()}


def set_geo(row_data, aPoint, hits, data):
    for hitIdx in hits:
        if data[hitIdx]['shape'].intersects(aPoint):
            if data[hitIdx]['type'] == 'watershed':
                #print "****** Watershed  **********"
                prop = data[hitIdx]['properties']
                #Legacy code - This make sense will test with list of watersheds
                if 'HUC_4' in prop:
                    row_data["huc_4"] = prop['HUC_4']
                if 'HUC_8' in prop:
                    row_data["huc_8"] = prop['HUC_8']
                #new format
                row_data['watersheds'].append({'name': prop['NAME'], 'HUC': prop['HUC']})
            else:  # Aquifers
                prop = data[hitIdx]['properties']
                #Legacy code - Errors replaced multiple aquifers with just one
                row_data["aquifer"] = prop['NAME']
                #new format
                row_data['aquifers'].append({'name': prop['NAME'], 'type': prop['TYPE']})
    return row_data


def get_webservice(site, db, url=config.usgs_site_metadata_url):
    ws_set = set([])
    data = []
    end_dates = []
    try:
        f = urllib2.urlopen(url % (site))
        f1 = StringIO.StringIO(f.read())
        temp = '#'
        head = ''
        while (temp[0] == "#"):
            temp = f1.readline()
            if temp[0] != '#':
                head = temp.strip('\r\n').split('\t')
        f1.readline()
        for r in f1:

            temp = r.strip('\r\n').split('\t')
            rec = dict(zip(head, temp))
            end_dates.append(rec['end_date'])
            try:
                param = db[site_database]['parameters'].find_one({'parameter_cd': rec['parm_cd']})
                data.append({'group_name': param['parameter_group_nm'], 'name': param['parameter_nm'],
                             'units': param['parameter_units'], 'parm_cd': rec['parm_cd'],
                             'webservice': rec['data_type_cd'], 'begin_date': rec['begin_date'],
                             'end_date': rec['end_date']})
            except:
                pass
            ws_set.add(rec['data_type_cd'])
        end_dates.sort()
        return list(ws_set), data, end_dates[-1]
    except:
        return list(ws_set), data, end_dates[-1]


#@task()
def update_webservie_types(database='ows', collection='usgs_site', delete=True):
    """*******************************"""
    db = Connection(mongoHost)
    url = 'http://waterservices.usgs.gov/nwis/site/?format=rdb&sites=%s&seriesCatalogOutput=true'
    for row in db[database][collection].find({'status': 'Active'}):
        ws_set = Set([])
        try:
            f = urllib2.urlopen(url % (row['site_no']))
            f1 = StringIO.StringIO(f.read())
            temp = '#'
            head = ''
            while (temp[0] == "#"):
                temp = f1.readline()
                if temp[0] != '#':
                    head = temp.strip('\r\n').split('\t')
            f1.readline()
            for r in f1:
                temp = r.strip('\r\n').split('\t')
                rec = dict(zip(head, temp))
                ws_set.add(rec['data_type_cd'])
            row['webservice'] = list(ws_set)
            db[database][collection].save(row)
        except:
            row['webservice'] = list(ws_set)
            db[database][collection].save(row)


@task()
def sites_usgs_wq(database=site_database, collection='usgs_wq_site', delete=True):
    '''

        Task to update USEPA and USGS 
        Water Quality sites

    '''
    db = Connection(mongoHost)
    #backup collection
    now = datetime.now()
    collection_backup = "%s_%s" % (collection, now.strftime("%Y_%m_%d_%H%M%S") )

    #get rtree spatial index and data object
    idx, geo_data = gis_tools.ok_watershed_aquifer_rtree()
    #if delete:
    #    db[database][collection].remove()
    url_site = config.wqp_site
    sites = urllib2.urlopen(url_site)
    output = StringIO.StringIO(sites.read())
    head = output.readline()
    head = head.replace('/', '-').strip('\r\n').split(',')
    reader = csv.DictReader(output, head)

    # set up metadata dataframe
    if os.path.isfile("%s/temp.zip" % config.wqp_tmp):
        os.remove("%s/temp.zip" % config.wqp_tmp)
    if os.path.isfile("%s/Result.csv" % config.wqp_tmp):
        os.remove("%s/Result.csv" % config.wqp_tmp)
    url_results = config.wqp_result_ok_all
    location = gis_tools.save_download(url_results, "%s/temp.zip" % config.wqp_tmp, compress='zip')
    df = pd.read_csv("%s/Result.csv" % (location), error_bad_lines=False)

    metadata_types = set([])
    metadata_analytes = set([])
    for rec in reader:
        rec['watersheds'] = []
        rec['aquifers'] = []
        rec["MonitoringLocationDescriptionText"] = filter(lambda x: x in string.printable,
                                                                  rec["MonitoringLocationDescriptionText"])
        rec["MonitoringLocationDescriptionText"] = rec["MonitoringLocationDescriptionText"].replace("\\u00bf", "")
        rec["MonitoringLocationDescriptionText"] = rec["MonitoringLocationDescriptionText"].replace("\u00bf", "")
        rec["MonitoringLocationDescriptionText"] = rec["MonitoringLocationDescriptionText"].replace("\\u00bf***", "")
        rec["MonitoringLocationDescriptionText"] = rec["MonitoringLocationDescriptionText"].replace("\u00bf***", "")
        rec["MonitoringLocationDescriptionText"] = rec["MonitoringLocationDescriptionText"].replace("\u00bf", "")
        metadata_types.add(rec['MonitoringLocationTypeName'].strip(' \t\n\r'))
        try:
            dfloc = df[df.MonitoringLocationIdentifier == rec['MonitoringLocationIdentifier']]
            #if len(df.index) != 0:
            #    rec['last_activity'] = df['ActivityStartDate'].max()
            #url = config.wqp_result % (rec['MonitoringLocationIdentifier'])
            #page = urllib2.urlopen(url)
            #df = pd.read_csv(page)
            if len(dfloc.index) != 0:
                rec['last_activity'] = dfloc['ActivityStartDate'].max()
                data = []
                grouped = dfloc.groupby('CharacteristicName')
                for idxs, row in grouped.agg([np.min, np.max]).iterrows():
                    metadata_analytes.add(idxs)
                    try:
                        pcode = "%05d" % row['USGSPCode']['amin']
                    except:
                        pcode = ''
                    data.append({'name': idxs, 'begin_date': row['ActivityStartDate']['amin'],
                                 'end_date': row['ActivityStartDate']['amax'],
                                 'parm_cd': pcode,
                                 'units': row['ResultMeasure/MeasureUnitCode']['amin']})
                rec['parameter'] = data
            else:
                #if no data skip site
                continue
        except:
            #if no data skip, may need to look at this is future
            continue
        try:
            x, y = gis_tools.transform_point(rec['LatitudeMeasure'], rec['LongitudeMeasure'])
            hits = list(idx.intersection((x, y, x, y)))
            aPoint = Point(x, y)
            rec = set_geo(rec, aPoint, hits, geo_data)
        except:
            #Legacy code inserted without lat lon and covered errors in code
            #decided to just pass and eliminate error catching. May change back in the future
            continue
        #insert when geo and parameters set
        db[database][collection_backup].insert(rec)
    rec = db[database]['catalog'].find_one({'metadata_source': 'wqp'})
    if rec:
        rec['types'] = metadata_types
        rec['analytes'] = metadata_analytes
    else:
        rec = {'metadata_source': 'wqp', 'types': list(metadata_types), 'parameters': list(metadata_analytes)}
    db[database]['catalog'].save(rec)
    return json.dumps({'source': 'usgs_wq', 'url': url_site, 'database': database, 'collection': collection_backup},
                      indent=2)


#@task()
def usgs_iv(database=site_database, collection='usgs_iv_site', delete=True):
    ''' Task to update USGS Sites
        Instantaneous Values Service for real-time data and historical data since October 1, 2007
     '''
    db = Connection(mongoHost)
    if delete:
        db[database][collection].remove()
    url = 'http://waterservices.usgs.gov/nwis/iv/?format=json&stateCd=ok'
    sites = urllib2.urlopen(url)
    data = json.loads(sites.read())
    uid = []
    for row in data['value']['timeSeries']:
        if not row['sourceInfo']['siteCode'][0]['value'] in uid:
            uid.append(row['sourceInfo']['siteCode'][0]['value'])
            temp = row['sourceInfo']
            temp['site_no'] = row['sourceInfo']['siteCode'][0]['value']
            db[database][collection].insert(temp)
    return json.dumps({'source': 'usgs_iv', 'url': url, 'database': database, 'collection': collection}, indent=2)


@task()
def usgs_parameters(database=site_database, collection=config.usgs_parameter_collection,
                    ws_url=config.usgs_parameter_url):
    ''' Task to update USGS Parameters '''
    db = Connection(mongoHost)
    now = datetime.now()
    collection_backup = "%s_%s" % (collection, now.strftime("%Y_%m_%d_%H%M%S") )
    db[database][collection].rename(collection_backup)
    url = ws_url
    param = urllib2.urlopen(url)
    output = StringIO.StringIO(param.read())
    temp = '#'
    head = ''
    while (temp[0] == "#"):
        temp = output.readline()
        if temp[0] != '#':
            head = temp.strip('\r\n').split('\t')
            output.readline()
    for row in output:
        temp = row.strip('\r\n').split('\t')
        db[database][collection].insert(dict(zip(head, temp)))
    return json.dumps({'source': 'params', 'url': url, 'database': database, 'collection': collection}, indent=2)


#@task()
def usgs_get_sitedata(sites, type='instantaneous', params="{'format':'json'}", data_provider='USGS'):
    dcommons = datacommons.toolkit(username, password)
    records = dcommons.get_data('ows', {'spec': {'data_provider': data_provider}, 'fields': ['sources']})
    sources = records[0]['sources']
    result = {}
    #http://waterservices.usgs.gov/nwis/iv/?format=json&sites=07230000&period=P1D&parameterCd=00060,00065
    #http://waterservices.usgs.gov/nwis/iv/?format=json&sites=07230000&startDT=2013-02-01&endDT=2013-02-22&parameterCd=00060,00065
    for source, val in sources.items():
        #src_url.append(val['url'])
        #for url in src_url:
        temp = ''
        #print params
        if source == type:
            param = json.loads(params.replace("'", '"'))
            for k, v in param.items():
                temp = temp + k + "=" + v + '&'

            url = val['url'] + temp + 'sites=' + sites
            #print url
            urlcheck = commands.getoutput("wget --spider '" + url + "' 2>&1| grep 'Remote file exists'")
            if urlcheck:
                try:
                    res = urllib2.urlopen(url)
                    if param['format'] == 'rdb':
                        data = res.read()
                        return data
                    else:
                        data = json.loads(res.read())
                        result[source] = {'url': url, 'data': data}
                except:
                    pass
            return json.dumps(result, indent=2)


