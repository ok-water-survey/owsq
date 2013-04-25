import ast,os,json,socket #,imp,inspect#,copy
import filezip 
import datetime
import ConfigParser
from subprocess import call
from celery.task import task
#from types import ListType
from celery.task import subtask
from celery.task import group
#set catalog user and passwd
cfgfile = os.path.join(os.path.expanduser('/opt/celeryq'), '.cybercom')
config= ConfigParser.RawConfigParser()
config.read(cfgfile)
username = config.get('user','username')
password = config.get('user','password')
log_info_tpl='INFO: Source - %s, Data Cart Items Submitted: %s, Status: %s \n'
log_info_tpl1='INFO: %s,Status: %s \n'
log_info_tpl2='INFO: %s: %s  Available: %s \n'
log_warn_tpl='WARNING-ERROR: %s \n'
zip_name_tpl='OWS_Data_%s.zip'
mongoHost = 'localhost'
site_database='ows'
@task()
def data_download(data,basedir='/data/static/',clustered=False,**kwargs):
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
    newDir = os.path.join(basedir,'ows_tasks/',str(data_download.request.id))
    call(["mkdir",'-p',newDir])
    os.chdir(newDir)
    logger = open(os.path.join(newDir,'task_log.txt'),'w')
    # consolidate sources- creates list of shopping cart items
    data_by_source={}
    for itm,value in data.items():
        value['query'] = ast.literal_eval(value['query'])
        if value['query']['source'] in data_by_source:
            data_by_source[value['query']['source']].append(value)
        else:
            data_by_source[value['query']['source']]=[value]
    stask=[]
    taskname_tmpl='owsq.data.download.%s.save'
    for itm, value in data_by_source.items():
        logger.write(log_info_tpl % (itm, str(len(value)),'Subtask Created'))
        stask.append(subtask(taskname_tmpl % (itm),args=(newDir,itm,),kwargs={'data_items':value}))
    job = group(stask)
    result = job.apply_async()
    logger.write(log_info_tpl1 % ('Subtask Submission','Subtask Running'))
    aggregate_results=result.join()
    logger.write(log_info_tpl1 % ('Data query Successful','Subtasks completed'))
    
    urls=[]
    for res in aggregate_results:
        urls.extend(res)
    outname = zip_name_tpl % (datetime.datetime.now().isoformat())
    zipurl ='http://%s/%s/%s' % (socket.gethostname(),'request', outname)
    logger.write(log_info_tpl2 % ('Data Zip URL',zipurl,'30 days'))
    logger.close()
    if clustered:
        return filezip.makezip(urls, zip_name_tpl % (datetime.datetime.now().isoformat()), os.path.join(basedir,'request/'))
    else:
        return filezip.makezip(newDir,zip_name_tpl % (datetime.datetime.now().isoformat()), os.path.join(basedir,'request/'),local=True)
