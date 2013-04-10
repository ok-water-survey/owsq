import ast,os,json,imp,inspect
import filezip
import ConfigParser,logging
from celery.utils.log import get_task_logger
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
@task()
def data_download(data=None,basedir='/data/static/'):
    '''
        Download multiple data sets from multiple data sources. 
            Simple cart data: Example
                {"SCI-1":{"quantity":1,"id":"SCI-1","name":"North Canadian River at Shawnee, OK (07241800)",
                          "parameter":"Discharge, cubic feet per second",
                           "query":"{'source':'USGS',  'webservice_type':'uv','sites':'07241800','parameterCd':'00060','startDT':'2007-10-01','endDT':'2013-04-04'}"}
                }
        query['source'] used to import module which will have a save function. THis function returns a url to file just downloaded.
        filezip creates a zip file from the list of urls
        Task returns a url to the zip file of all data downloaded from different sources 
        Currently performing in a serial fashion. Need to update and perform with celery groups in which multiple parallel subtask are generated.
        
    '''
    if not data:
        raise 'No Data'
    data = json.loads(data)
    module_dir=os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    newDir = os.path.join(basedir,'ows_tasks/',str(data_download.request.id))
    call(["mkdir",newDir])
    os.chdir(newDir)
    call(["touch",os.path.join(newDir,'task_log.txt')])
    logger = data_download.get_logger(logfile=os.path.join(newDir,'task_log.txt'))
    urls=[]
    for itm,value in data.items():
        item = ast.literal_eval(value['query'])
        try:
            query = ast.literal_eval(value['query'])
            logger.info(value['name'] + ' -ParamCode:' + query['parameterCd'] + ' - STARTED')
            data_import=imp.load_source(item['source'],os.path.join(module_dir,item['source'] + '.py')) 
            urls.append(data_import.save(value['name'],newDir,query))
            logger.info(value['name'] + ' -ParamCode:' + query['parameterCd'] + ' - FINISHED')
        except Exception as inst:
            logger.warning(inst)
            raise inst
    return filezip.makezip(urls, str(data_download.request.id)+ '.zip', os.path.join(basedir,'request/'))
