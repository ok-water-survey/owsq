import os,urllib2,urllib
#import ConfigParser
from cybercom.data.catalog import datacommons #catalog
#from owsq.data.download import filezip
from subprocess import call
from celery.task import task
from owsq import config
import dateutil.parser
from pymongo import Connection
#set catalog user and passwd
username = config.catalog_username #s.get('user','username')
password = config.catalog_password #s.get('user','password')

@task
def save(path,source,data_items=[]):#name,path,query):
    '''Based function to all source imports in Download module'''
    dcommons = datacommons.toolkit(username,password)
    consol_data=consolidate(data_items)
    sourcepath = os.path.join(path,'OWRB','Monitor_Wells')
    call(['mkdir','-p',sourcepath])
    urls=[]
    database=config.owrb_database
    collection=config.owrb_MonitorWells_collection
    host = get_host(dcommons)
    urlbase= host['base_directory']
    owrb_url="http://test.oklahomawatersurvey.org/mongo/db_find/ows/owrb_monitoring_wells/{'spec':{'site':'%s'},'field':['']}/?outtype=csv" 
    meso_url="http://www.mesonet.org/index.php/meteogram/data/owrb_text//stid/%s/year/%s/month/%s/day/%s/timelen/%sd/product/GH20/type/csv"
    db=Connection(config.mongo_host)

    for key,value in consol_data.items():
        if value['query']['webservice_type']==mesonet:
            filename='OWRB_MonitoringWell_mesonet%s.csv' % (value['query']['sites'])
            sitedata=db[database]['owrb_monitor_sites'].find_one({'WELL_ID':value['query']['sites']})
            mesosite=sitedata['mesonetID']
            start=dateutil.parser.parse(value['query']['startDT'])
            end =dateutil.parser.parse(value['query']['endDT'])
            day_count = (end - start).days + 1
            month = end.strftime("%m")
            year = end.strftime("%Y")
            day = end.strftime("%d")            
            url= meso_url % (mesosite,year,month,day,day_count)
            
            f1=open(os.path.join(sourcepath,filename),'w')
            res=urllib2.urlopen(url)
            f1.write(res.read())
            f1.close()
            urls.append(os.path.join(sourcepath.replace(urlbase ,host['url']),filename))
        else:#
            filename='OWRB_MonitoringWell_%s.csv' % (value['query']['sites'])
            f1=open(os.path.join(sourcepath,filename),'w')
            head="site,date,measurement,unit,status,project\n"
            f1.write(head)
            temp_tmpl="%s,%s,%s,%s,%s,%s\n"
            for row in db[database][collection].find({'site':value['query']['sites']}).sort([('sort_date',-1),]):
                temp = temp_tmpl % (row['site'],row['observed_date'],row['value'],row['unit'],row['status'],row['project'])
                f1.write(temp)
            f1.close()
            urls.append(os.path.join(sourcepath.replace(urlbase ,host['url']),filename))
    return urls
def consolidate(data_items):
    cons_queries={}
    for item in data_items:
        node="%s_%s" % (item['query']['sites'],item['query']['webservice_type'])
        if node not in cons_queries:
            cons_queries[node]=item
        else:
            if cons_queries[node]['query']['startDT']>item['query']['startDT']:
                cons_queries[node]['query']['startDT']=item['query']['startDT']
            if cons_queries[node]['query']['endDT']<item['query']['endDT']:
                cons_queries[node]['query']['endDT']=item['query']['endDT']
    return cons_queries
def get_host(dcommons):
    hosts = dcommons.get_data('ows',{'spec':{'data_provider':'APP_HOSTS'},'fields':['sources']})[0]['sources']
    for item in(item for item in hosts if item['host']==os.uname()[1]):
        return item
    raise 'No Host specified, Please upadate Catalog'
