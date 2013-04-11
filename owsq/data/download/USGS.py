import os,commands,urllib2
import ConfigParser #,logging
from cybercom.data.catalog import datacommons #catalog
#import filezip
from cybercom.util.convert import csvfile_processor

#set catalog user and passwd
cfgfile = os.path.join(os.path.expanduser('/opt/celeryq'), '.cybercom')
config= ConfigParser.RawConfigParser()
config.read(cfgfile)
username = config.get('user','username')
password = config.get('user','password')


def save(name,path,query):
    '''Based function to all source imports in Download module'''
    temp=query
    temp.pop('source')
    return save_sitedata(name,path,temp)
def save_csv(url,path,filezip):
    dcommons = datacommons.toolkit(username,password)
    data = filezip.rdb2json(url)
    fileName, fileExtension = os.path.splitext( url.split('/')[-1])
    fileExtension='.csv'
    filename= fileName + fileExtension
    f1=open(os.path.join(path,filename),'w')
    f1.write(csvfile_processor(data))
    f1.close()
    host=get_host(dcommons)
    return os.path.join(path.replace(host['base_directory'],host['url']),filename)

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
        raise 'No Host specified, Please upadate Catalog'
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
    print url
    urlcheck = commands.getoutput("wget --spider '" + url + "' 2>&1| grep 'Remote file exists'")
    if urlcheck:
        try:
            res=urllib2.urlopen(url)
            filename= sites + '_parameterCd-' + query['parameterCd'] + '.txt'
            f1=open(os.path.join(path,filename),'w')
            f1.write(res.read())
            urlbase= host['base_directory']
            return os.path.join(path.replace(urlbase ,host['url']),filename)
        except Exception as inst:
            raise inst
    else:
        raise 'URL ERROR: ' + url     
