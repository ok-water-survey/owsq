import os,urllib2,urllib,json
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
    locid=consolidate(data_items)
    #locid=[]
    print locid
    sourcepath = os.path.join(path,source)
    call(['mkdir','-p',sourcepath])
    #urls=[]
    database=config.owrb_database
    collection=config.occ_site_data
    urltemp = "http://test.oklahomawatersurvey.org/mongo/db_find/%s/%s/{'spec':{'Location_id':'%s'}}/?outtype=csv"
    urllist = []
    for locquery in locid:
        print locquery
        #data = json.loads(locquery)
        #print data
        url = urltemp % (database,collection,locquery)
        print url
        res=urllib2.urlopen(url)
        filename='OCC_Data_%s.csv' % locquery
        f1=open(os.path.join(sourcepath,filename),'w')
        f1.write(res.read())
        f1.close()
        host = get_host(dcommons)
        urlbase= host['base_directory']
        urllist.append(os.path.join(sourcepath.replace(urlbase ,host['url']),filename))
    return urllist
def consolidate(data_items):
    locids=[]
    for item in data_items:
        locids.append(item['query']['webservice_type'])
    return locids
def get_host(dcommons):
    hosts = dcommons.get_data('ows',{'spec':{'data_provider':'APP_HOSTS'},'fields':['sources']})[0]['sources']
    for item in(item for item in hosts if item['host']==os.uname()[1]):
        return item
    raise 'No Host specified, Please upadate Catalog'
