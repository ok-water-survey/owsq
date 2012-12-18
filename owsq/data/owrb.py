import json,urllib2,ConfigParser,os
from subprocess import call
from glob import glob
from celery.task import task
from celery.task.sets import subtask
from pymongo import Connection
from datetime import datetime,timedelta
from cybercom.data.catalog import datacommons #catalog
#set catalog user and passwd
cfgfile = os.path.join(os.path.expanduser('/opt/celeryq'), '.cybercom')
config= ConfigParser.RawConfigParser()
config.read(cfgfile)
username = config.get('user','username')
password = config.get('user','password')

#set Mongo Host and default database
mongoHost = 'localhost'
site_database='ows'

@task()
def owrb_sync_geojson(data_type='groundwater',database=site_database,tmp_fldr='/data/owrb/',data_provider='OWRB',delete=True):
    ''' Load OWRB shape files and convert to geojson and store on static web server. Catalog location so avaialbe for applications'''
    dcommons = datacommons.toolkit(username,password)
    records= dcommons.get_data('ows',{'spec':{'data_provider':data_provider}},showids=True)
    sources = records[0]['sources']
    result={}
    if not data_type in sources:
        dt=[]
        for source in sources:
            dt.append(source)
        return json.dumps({'status':'Error - Unkown data_type','available data_types': dt} , indent=2 )
    for source,val in sources[data_type].items():
        url =val['url'] 
        res=urllib2.urlopen(url)
        file_dl= tmp_fldr + source + '.zip'
        output= open(file_dl,'wb')
        output.write(res.read())
        output.close()
        call(['unzip','-o', file_dl , '-d', tmp_fldr + source])
        ows_url =[]
        for fl in glob(tmp_fldr + source +'/*.shp'): 
            shpfile = fl 
            outfile = shpfile.split('.')[0] + '.json'
            fname=os.path.basename(outfile)
            if os.path.exists(outfile):
                call(['rm',outfile])
            call(['ogr2ogr','-f','GeoJSON','-t_srs','EPSG:3857', outfile , shpfile])
            call(['scp',outfile, "mstacy@static.cybercommons.org:/static/OklahomaWaterSurvey/OWRB/geojson/" + fname])
            data_url ='http://static.cybercommons.org/OklahomaWaterSurvey/OWRB/geojson/' + fname
            ows_url.append(data_url)
        sources[data_type][source]['ows_url'] = ows_url
        result[source]={'source':source,'url':url,'geojson':ows_url}
    dcommons.save('ows',records[0])
    return json.dumps( result, indent=2 )
