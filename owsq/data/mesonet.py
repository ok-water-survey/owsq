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
    """ Update mesonet sites """
    db=Connection(config.mesonet_mongo_host)
    reader = csv.reader(urllib2.urlopen(url))
    data=[]
    now = datetime.now()
    collection_backup = "%s_%s" % (collection, now.strftime("%Y_%m_%d_%H%M%S") )
    db[database][collection].rename(collection_backup)
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

@task()
def mesonet_sites_cleanup(database=config.mesonet_database,collection=config.mesonet_collection): 
    """
    Clean up backup site collections. Update renames site collection plus date updated.
    Finds site collection and keeps the last two backups.
    """
    db=Connection(config.mesonet_mongo_host)
    r_list =[]
    for col in db[database].collection_names():
        if col.split(colletion)[0] == '':
            if col != collection:
                r_list.append(col)
    r_list.sort()
    while len(r_list)>2:
        rm_coll = r_list.pop(0)
        db[database][rm_coll].remove()
        
    
            
