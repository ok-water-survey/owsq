import os
import ConfigParser
from cybercom.data.catalog import datacommons #catalog
from owsq.data.download import filezip
from subprocess import call
from celery.task import task
import dateutil.parser
from datetime import  timedelta
#set catalog user and passwd
cfgfile = os.path.join(os.path.expanduser('/opt/celeryq'), '.cybercom')
config= ConfigParser.RawConfigParser()
config.read(cfgfile)
username = config.get('user','username')
password = config.get('user','password')

@task
def save(path,source,data_items=[]):#name,path,query):
    '''Based function to all source imports in Download module'''
    con_query=consolidate(data_items)
    sourcepath = os.path.join(path,source)
    call(['mkdir','-p',sourcepath])
    urls=[]
    for key,value in con_query.items():
        return_url= save_sitedata(sourcepath,value['query'])
        urls.extend(return_url)
        name = value['name'].replace(' ','').replace('(','_').replace(')','').replace(',','') +'.csv'
        return_url=save_csv(return_url,sourcepath,name)
        urls.append(return_url)
    return urls
def consolidate(data_items):
    cons_queries={}
    for item in data_items:
        node="%s" % (item['query']['sites'])
        if node not in cons_queries:
            cons_queries[node]=item
        else:
            if cons_queries[node]['query']['startDT']>item['query']['startDT']:
                cons_queries[node]['query']['startDT']=item['query']['startDT']
            if cons_queries[node]['query']['endDT']<item['query']['endDT']:
                cons_queries[node]['query']['endDT']=item['query']['endDT'] 
    return cons_queries
def save_csv(urls,path,name):
    dcommons = datacommons.toolkit(username,password)
    data,ordercol,head = filezip.meso2json(urls)
    f1=open(os.path.join(path,name),'w')
    f1.write(filezip.csvfile_processor(data,cols=ordercol,header=head))
    f1.close()
    host=get_host(dcommons)
    return os.path.join(path.replace(host['base_directory'],host['url']),name)
 
def get_host(dcommons):
    hosts = dcommons.get_data('ows',{'spec':{'data_provider':'APP_HOSTS'},'fields':['sources']})[0]['sources']
    for item in(item for item in hosts if item['host']==os.uname()[1]):
        return item
    raise 'No Host specified, Please upadate Catalog'

def save_sitedata(path,query):
    url_tmpl = 'http://www.mesonet.org/index.php/dataMdfMts/dataController/getFile/%s%s/mts/TEXT/'
    #Load source web service data from metadata catalog
    dcommons = datacommons.toolkit(username,password)
    host = get_host(dcommons)
    urlbase= host['base_directory']
    start=dateutil.parser.parse(query['startDT'])
    end =dateutil.parser.parse(query['endDT'])
    site = query['site_no'].lower()
    day_count = (end - start).days + 1
    rpts=[]
    for single_date in (start + timedelta(n) for n in range(day_count)):
        rpts.append(url_tmpl % (single_date.strftime('%Y%m%d'),site))
    #rpts=query['special']
    newpath= '%s/%s' % (path,query['sites'])
    call(['mkdir','-p',newpath])
    urls=[]
    #.split('/')[-4]
    for url  in rpts:
        name=url.split('/')[-4]
        call(['wget','-O',"%s/%s.txt" % (newpath,name),url])
        urls.append(os.path.join(path.replace(urlbase , host['url']),name))
    return urls
