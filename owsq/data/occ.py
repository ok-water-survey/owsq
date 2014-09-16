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

@task()
def sites_occ_update(database=site_database, collection=config.occ_site_collection):  #,delete=True):
    '''**************************
        Task to update OCC 
        Sites
    '''
    db = Connection(mongoHost)
    #backup collection
    now = datetime.now()
    
    #get rtree spatial index and data object
    idx, data = gis_tools.ok_watershed_aquifer_rtree()
    for rec in db[database][collection].find():
	    #set watershed and aquifer
	    try:
		x, y = gis_tools.transform_point(rec['Lat'], rec['Long'])
		hits = list(idx.intersection((x, y, x, y)))  #, objects=True)) #[0]  #[0].object
		aPoint = Point(x, y)
                rec['watersheds']=[]
                rec['aquifers']=[]
                rec['huc_8']=""
                rec['huc_4']=""
		row_data = set_geo(rec, aPoint, hits, data)
		#Save site data
                db[database][collection].save(row_data)
	    except Exception as err:
                pass
    return {'source': 'occ', 'database': database,'collection': collection,
		'record_count': db[database][collection].count()}


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

