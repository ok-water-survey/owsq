import ast,os,json,imp,inspect
import filezip 
import datetime
import ConfigParser
from subprocess import call
from celery.task import task
from types import ListType

#set catalog user and passwd
cfgfile = os.path.join(os.path.expanduser('/opt/celeryq'), '.cybercom')
config= ConfigParser.RawConfigParser()
config.read(cfgfile)
username = config.get('user','username')
password = config.get('user','password')
log_info_tpl='******INFO: %s ParamCode: %s - Status: %s ******\n'
log_warn_tpl='******WARNING-ERROR: %s ******\n'
zip_name_tpl='OWS_Data_%s.zip'
mongoHost = 'localhost'
site_database='ows'
@task()
def data_download(data=None,basedir='/data/static/',clustered=False):
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
        raise Exception('No Data')
    try:
        data = json.loads(data)
    except:
        data= ast.literal_eval(data)
    module_dir=os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    newDir = os.path.join(basedir,'ows_tasks/',str(data_download.request.id))
    call(["mkdir",'-p',newDir])
    os.chdir(newDir)
    logger = open(os.path.join(newDir,'task_log.txt'),'w')
    urls=[]
    for itm,value in data.items():
        item = ast.literal_eval(value['query'])
        try:
            query = ast.literal_eval(value['query'])
            logger.write(log_info_tpl % (value['name'], query['parameterCd'],'STARTED'))
            data_import=imp.load_source(item['source'],'%s.py' % (os.path.join(module_dir,item['source']))) 
            return_url=data_import.save(value['name'],newDir,query)
            if type(return_url) is ListType:
                urls.extend(return_url)
            else:
                urls.append(return_url)
            csv= data_import.save_csv(return_url,newDir,query)
            if csv:
                urls.append(csv)
            logger.write(log_info_tpl % (value['name'], query['parameterCd'],'FINISHED'))
        except Exception as inst:
            logger.write(log_warn_tpl % (str(inst)))
            raise inst
    if clustered:
        return filezip.makezip(urls, zip_name_tpl % (datetime.datetime.now().isoformat()), os.path.join(basedir,'request/'))
    else:
        return filezip.makezip(newDir,zip_name_tpl % (datetime.datetime.now().isoformat()), os.path.join(basedir,'request/'),local=True)
