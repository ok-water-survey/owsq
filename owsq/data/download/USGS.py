import os,commands,urllib2,copy#,ast
import ConfigParser
from cybercom.data.catalog import datacommons #catalog
from owsq.data.download import filezip
from subprocess import call
from celery.task import task

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
        
        if value['query']['webservice_type']!='ad':
            name = value['name'].replace(' ','').replace('(','_').replace(')','_').replace(',','') + value['query']['webservice_type']
            query = copy.deepcopy(value['query'])
            query.pop('source')
            return_url=save_sitedata(name,sourcepath,query)
            urls.append(return_url)
            query = copy.deepcopy(value['query'])
            return_url=save_csv(return_url,sourcepath,query)
            if return_url:
                urls.append(return_url)
        else:
            return_url=save_reports(sourcepath,value['query'])
            urls.extend(return_url)        
    return urls
def consolidate(data_items):
    cons_queries={}
    for item in data_items:
        node="%s_%s" % (item['query']['webservice_type'],item['query']['sites'])
        if node not in cons_queries:
            cons_queries[node]=item
        else:
            if item['query']['webservice_type']!='ad':
                cons_queries[node]['query']['parameterCd']="%s%s%s" % (cons_queries[node]['query']['parameterCd'],",",item['query']['parameterCd'])
                if cons_queries[node]['query']['startDT']>item['query']['startDT']:
                    cons_queries[node]['query']['startDT']=item['query']['startDT']
                if cons_queries[node]['query']['endDT']<item['query']['endDT']:
                    cons_queries[node]['query']['endDT']=item['query']['endDT'] 
    return cons_queries
def save_csv(url,path,query):
    if query['webservice_type']!='ad':
        dcommons = datacommons.toolkit(username,password)
        data,ordercol,head = filezip.rdb2json(url)
        fileName, fileExtension = os.path.splitext( url.split('/')[-1])
        fileExtension='.csv'
        filename= fileName + fileExtension
        f1=open(os.path.join(path,filename),'w')
        f1.write(filezip.csvfile_processor(data,cols=ordercol,header=head))
        f1.close()
        host=get_host(dcommons)
        return os.path.join(path.replace(host['base_directory'],host['url']),filename)
    return None
def get_host(dcommons):
    hosts = dcommons.get_data('ows',{'spec':{'data_provider':'APP_HOSTS'},'fields':['sources']})[0]['sources']
    for item in(item for item in hosts if item['host']==os.uname()[1]):
        return item
    raise 'No Host specified, Please upadate Catalog'
 
def save_sitedata(name,path,query,data_provider='USGS-Tools-TypeSet',default_format='rdb'):
    '''Load data from USGS websevice and store local NGINX web server. Returns url of file'''
    #Load source web service data from metadata catalog
    dcommons = datacommons.toolkit(username,password)
    sources = dcommons.get_data('ows',{'spec':{'data_provider':data_provider}})[0]
    #Get Host information - NGINX root and urls from metadata catalog
    host=None
    hosts = dcommons.get_data('ows',{'spec':{'data_provider':'APP_HOSTS'},'fields':['sources']})[0]['sources']
    for item in(item for item in hosts if item['host']==os.uname()[1]):
        host=item
    if not host:
        raise Exception('No Host specified, Please upadate Catalog')
    sites=query['sites']
    params=query.copy()
    if not 'format' in params:
        params['format'] = default_format #default format
    params.pop('sites')
    #Setup metadata web service from data catalog
    metadata = sources[query['webservice_type']]
    params.pop('webservice_type')
    temp=''
    for k,v in params.items():
        temp= temp + k + "=" + v + '&'
    url =metadata['webservice'] + temp + 'sites=' + sites
    urlcheck = commands.getoutput("wget --spider '" + url + "' 2>&1| grep 'Remote file exists'")
    if urlcheck:
        try:
            res=urllib2.urlopen(url)
            filename= "%s.txt" % (name) 
            f1=open(os.path.join(path,filename),'w')
            f1.write(res.read())
            urlbase= host['base_directory']
            return os.path.join(path.replace(urlbase ,host['url']),filename)
        except Exception as inst:
            raise inst
    else:
        raise Exception('URL ERROR: ' + url)

def save_reports(path,query):
    #Load source web service data from metadata catalog
    dcommons = datacommons.toolkit(username,password)
    host = get_host(dcommons)
    urlbase= host['base_directory']
    rpts=query['special']
    newpath= '%s/%s' % (path,'reports')
    call(['mkdir','-p',newpath])
    urls=[]
    for key,val in rpts.items():
        call(['wget','-P',newpath,val])
        name= val.split('/')[-1]
        urls.append(os.path.join(path.replace(urlbase , host['url']),name))
    return urls            
