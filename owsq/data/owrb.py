import json, urllib2, os  #,ConfigParser,os
from subprocess import call
from glob import glob
from celery.task import task
#from celery.task.sets import subtask
from pymongo import Connection
#from datetime import datetime,timedelta
from cybercom.data.catalog import datacommons  #catalog
from owsq import config
from owsq.util import gis_tools
from celery.task import subtask
from celery.task import group
from shapely.geometry import Point
#set catalog user and passwd
username = config.catalog_username  #s.get('user','username')
password = config.catalog_password  #s.get('user','password')
watershed = None
aquifer = None


@task()
def owrb_set_geo_latest(database=config.owrb_database, collection=config.owrb_site_collection,
                        collection_data=config.owrb_well_collection):
    db = Connection(config.mongo_host)

    #get rtree spatial index and data object
    idx, data = gis_tools.ok_watershed_aquifer_rtree()

    total_ct = 0
    update_ct = 0

    for site in db[database][collection].find():
        site['watersheds'] = []
        site['aquifers'] = []
        site["huc_4"] = ''
        site["huc_8"] = ''
        site["aquifer"] = ''
        site['last_activity'] = ''
        total_ct += 1

        #set watershed and aquifer
        try:
            try:
                dates = db[database][collection_data].distinct('observed_date', {'site': str(site['WELL_ID'])})
                dates.sort()
                site['last_activity'] = dates[-1]
            except:
                pass

            x, y = gis_tools.transform_point(site['LATITUDE'], site['LONGITUDE'])
            hits = list(idx.intersection((x, y, x, y)))
            aPoint = Point(x, y)
            row_data = set_geo(site, aPoint, hits, data)
            update_ct += 1
            db[database][collection].save(row_data)
        except:
            db[database][collection].save(site)
    return {'total_count': total_ct, 'updated_count': update_ct}


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


#@task()
def owrb_sync_geojson(data_type='groundwater', database=config.owrb_database, tmp_fldr='/data/owrb/',
                      data_provider='OWRB', delete=True):
    ''' Load OWRB shape files and convert to geojson and store on static web server. Catalog location so avaialbe for applications'''
    dcommons = datacommons.toolkit(username, password)
    records = dcommons.get_data('ows', {'spec': {'data_provider': data_provider}}, showids=True)
    sources = records[0]['sources']
    result = {}
    if not data_type in sources:
        dt = []
        for source in sources:
            dt.append(source)
        return json.dumps({'status': 'Error - Unkown data_type', 'available data_types': dt}, indent=2)
    for source, val in sources[data_type].items():
        url = val['url']
        res = urllib2.urlopen(url)
        file_dl = tmp_fldr + source + '.zip'
        output = open(file_dl, 'wb')
        output.write(res.read())
        output.close()
        call(['unzip', '-o', file_dl, '-d', tmp_fldr + source])
        ows_url = []
        for fl in glob(tmp_fldr + source + '/*.shp'):
            shpfile = fl
            outfile = shpfile.split('.')[0] + '.json'
            fname = os.path.basename(outfile)
            if os.path.exists(outfile):
                call(['rm', outfile])
            call(['ogr2ogr', '-f', 'GeoJSON', '-t_srs', 'EPSG:3857', outfile, shpfile])
            call(['scp', outfile, "mstacy@static.cybercommons.org:/static/OklahomaWaterSurvey/OWRB/geojson/" + fname])
            data_url = 'http://static.cybercommons.org/OklahomaWaterSurvey/OWRB/geojson/' + fname
            ows_url.append(data_url)
        sources[data_type][source]['ows_url'] = ows_url
        result[source] = {'source': source, 'url': url, 'geojson': ows_url}
    dcommons.save('ows', records[0])
    return json.dumps(result, indent=2)


#@task()
def owrb_well_logs_save(database=config.owrb_database, collection=config.owrb_welllog_collection):
    #dcommons = datacommons.toolkit(username,password)
    db = Connection(config.mongo_host)
    db[database][collection].remove()
    #set geometries
    polydata = []
    for itm in db.ows.watersheds.find():
        polydata.append(itm)
    aquifer_poly = []
    for itm in db.ows.aquifers.find():
        aquifer_poly.append(itm)
    #load owrb well logs
    res = urllib2.urlopen(config.well_logs_url)
    data = json.loads(res.read())
    stask = []
    taskname_tmpl = 'owsq.data.owrb.owrb_well_logs_portal'
    for site in data["features"]:
        row_data = {}
        row_data = site["properties"]
        row_data['geometry'] = site['geometry']
        rowid = db[database][collection].save(row_data)
        stask.append(subtask(taskname_tmpl, args=(rowid,)))
    print 'Done with inserts, starting group jobs'
    job = group(stask)
    result = job.apply_async()
    aggregate_results = result.join()
    return "Success- All Well logs stored locally in Mongo(%s, %s) Total = %d" % (
    database, collection, sum(aggregate_results))


#@task()
def owrb_well_logs_portal(rowid, database=config.owrb_database, collection=config.owrb_welllog_collection, **kwargs):
    global watershed, aquifer
    db = Connection(config.mongo_host)
    #set watershed and aquifer geodata
    if watershed:
        polydata = watershed
    else:
        polydata = []
        for itm in db.ows.watersheds.find():
            polydata.append(itm)
        watershed = polydata
    if aquifer:
        aquifer_poly = aquifer
    else:
        aquifer_poly = []
        for itm in db.ows.aquifers.find():
            aquifer_poly.append(itm)
        aquifer = aquifer_poly
    row_data = db[database][collection].find_one({'_id': rowid})
    row_data["huc_4"] = ''
    row_data["huc_8"] = ''
    for poly in polydata:
        s = poly['geometry']
        if gis_tools.intersect_point(s, row_data['LATITUDE'], row_data['LONGITUDE']):
            if 'HUC_4' in poly['properties']:
                row_data["huc_4"] = poly['properties']['HUC_4']
            if 'HUC_8' in poly['properties']:
                row_data["huc_8"] = poly['properties']['HUC_8']
    #set aquifer data
    row_data["aquifer"] = ''
    for poly in aquifer_poly:
        s = poly['geometry']
        if gis_tools.intersect_point(s, row_data['LATITUDE'], row_data['LONGITUDE']):
            row_data["aquifer"] = poly['properties']['NAME']
            break
    db[database][collection].save(row_data)
    return 1


#@task()
def owrb_well_logs_sync():
    result = owrb_well_logs_save.delay(callback=subtask(owrb_well_logs_portal))
    return result

