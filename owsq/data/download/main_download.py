import ast,os,json,imp
import ConfigParser,logging
from subprocess import call
#import json,urllib2,StringIO,csv,ConfigParser,os,commands
from celery.task import task
from celery.task.sets import subtask
#from celery import chord
#from pymongo import Connection
#from datetime import datetime,timedelta
#from cybercom.data.catalog import datacommons #catalog
#set catalog user and passwd
cfgfile = os.path.join(os.path.expanduser('/opt/celeryq'), '.cybercom')
config= ConfigParser.RawConfigParser()
config.read(cfgfile)
username = config.get('user','username')
password = config.get('user','password')

mongoHost = 'localhost'
site_database='ows'
basedir =  '/data/static/ows_tasks/'
@task()
def data_download(data=None):
    '''
        Download multiple data sets from multiple data sources. 
            Simple cart data: Example
                {"SCI-1":{"quantity":1,"id":"SCI-1","name":"North Canadian River at Shawnee, OK (07241800)",
                          "parameter":"Discharge, cubic feet per second",
                           "query":"{'source':'USGS',  'webservice_type':'uv','sites':'07241800','parameterCd':'00060','startDT':'2007-10-01','endDT':'2013-04-04'}"}
                }
    '''
    #data= '''{"SCI-1":{"quantity":1,"id":"SCI-1","name":"North Canadian River at Shawnee, OK (07241800)","parameter":"Discharge, cubic feet per second","query":"{'source':'USGS','webservice_type':'uv','sites':'07241800','parameterCd':'00060','startDT':'2007-10-01','endDT':'2013-04-04'}"}}'''
    if not data:
        raise 'No Data'
    data = json.loads(data)
    mod_dir=os.getcwd()
    newDir = os.path.join(basedir,str(data_download.request.id))
    call(["mkdir",newDir])
    logging.basicConfig(filename=os.path.join(newDir,'task_log.txt'),level=logging.DEBUG)
    #call(["mkdir",newDir])
    os.chdir(newDir)
    #dcommons = datacommons.toolkit(username,password)
    #records= dcommons.get_data('ows',{'spec':{'data_provider':data_provider},'fields':['sources']})

    for itm,value in data.items():
        item = ast.literal_eval(value['query'])
        try:
            query = ast.literal_eval(value['query'])
            logging.info(value['name'] + ' -ParamCode:' + query['parameterCd'] + ' - STARTED')
            #print os.path.join(mod_dir,'USGS'+'.py')
            data_import=imp.load_source(item['source'],os.path.join(mod_dir,'USGS'+'.py')) # __import__(os.path.join(pwd,'USGS'))#item['source'])
            data_import.save(value['name'],newDir,query)
            logging.info(value['name'] + ' -ParamCode:' + query['parameterCd'] + ' - FINISHED')
        except Exception as inst:
            logging.warning(inst)
            raise inst
            #print "error !!!!!!!"
            
        
#        if item['source']=='USGS':
#            data_import= __import__(item['source'])
#            temp = item
#            temp.pop('source')
#            if item['webservice_type']=='uv' or item['webservice_type']=='iv':
#                temp.pop('webservice_type')
#                temp['format']='json'
#                print temp
#                result=subtask('owsq.data.usgs.usgs_get_sitedata',(temp['sites'],), {'type':'instantaneous','params':temp,'save':'static'}) 
#    return result
