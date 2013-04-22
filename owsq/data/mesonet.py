import urllib2,csv,ConfigParser,os
from datetime import datetime
from celery.task import task
from pymongo import Connection
from owsq import config
from owsq.util import gis_tools

#set catalog user and passwd
cfgfile = os.path.join(os.path.expanduser('/opt/celeryq'), '.cybercom')
uconfig= ConfigParser.RawConfigParser()
uconfig.read(cfgfile)
username = uconfig.get('user','username')
password = uconfig.get('user','password')


@task()
def update_mesonet_sites(url=config.mesonet_site_url,database=config.mesonet_database,collection=config.mesonet_collection):
    db=Connection(config.mesonet_mongo_host)
    reader = csv.reader(urllib2.urlopen(url))
    data=[]
    db[database][collection].remove()
    now = datetime.now()
    polydata=[]
    for itm in db.ows.watersheds.find():
        polydata.append(itm)
    aquifer_poly=[]
    for itm in db.ows.aquifers.find():
        aquifer_poly.append(itm)
    for row in reader:
        if reader.line_num == 1:
            headers = row[1:]
        else:
            row_data=dict(zip(headers, row[1:]))
            if row_data['nlat'] != '':
                #set watershed values
                for poly in polydata:
                    s= poly['geometry']
                    if gis_tools.intersect_point(s,row_data['nlat'],row_data['elon']):
                        if 'HUC_4' in poly['properties']:
                            #print 'HUC 4: ' + poly['properties']['HUC_4']
                            row_data["huc_4"]=poly['properties']['HUC_4']
                        if 'HUC_8' in poly['properties']:
                            #print 'HUC 8: ' + poly['properties']['HUC_8']
                            row_data["huc_8"]=poly['properties']['HUC_8']
                #set aquifer data
                for poly in aquifer_poly:
                    s= poly['geometry']
                    if gis_tools.intersect_point(s,row_data['nlat'],row_data['elon']):
                        row_data["aquifer"]=poly['properties']['NAME']
                        print poly['properties']['NAME']
                        break
                #set status
                date_decom = datetime.strptime(row_data["datd"], "%Y%m%d")
                if date_decom <= now:
                    row_data["status"]="Inactive"
                else:
                    row_data["status"]="Active"
                #save data to database
                data.append(row_data)
                db[database][collection].save(row_data)
    return True 
