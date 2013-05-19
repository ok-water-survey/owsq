import os,urllib2,urllib
#import ConfigParser
from cybercom.data.catalog import datacommons #catalog
#from owsq.data.download import filezip
from subprocess import call
from celery.task import task
from owsq import config

#set catalog user and passwd
username = config.catalog_username #s.get('user','username')
password = config.catalog_password #s.get('user','password')

@task
def save(path,source,data_items=[]):#name,path,query):
    '''Based function to all source imports in Download module'''
    dcommons = datacommons.toolkit(username,password)
    counties=consolidate(data_items)
    sourcepath = os.path.join(path,source)
    call(['mkdir','-p',sourcepath])
    #urls=[]
    database=config.owrb_database
    collection=config.owrb_welllog_collection 
    url= "http://test.oklahomawatersurvey.org/mongo/db_find/" + database + "/" + collection + "/{'spec':{'COUNTY':{'$in':" + str(counties) +  "}}}/?outtype=csv"   
    print url
    res=urllib2.urlopen(urllib.quote(url))
    filename='OWRB_WellLogs.csv'
    f1=open(os.path.join(sourcepath,filename),'w')
    f1.write(res.read())
    f1.close()
    host = get_host(dcommons)
    urlbase= host['base_directory']
    urls=os.path.join(sourcepath.replace(urlbase ,host['url']),filename)
    print urls
    return urls
def consolidate(data_items):
    county=[]
    for item in data_items:
        county.append(item['query']['webservice_type'])
    return county
def get_host(dcommons):
    hosts = dcommons.get_data('ows',{'spec':{'data_provider':'APP_HOSTS'},'fields':['sources']})[0]['sources']
    for item in(item for item in hosts if item['host']==os.uname()[1]):
        return item
    raise 'No Host specified, Please upadate Catalog'
