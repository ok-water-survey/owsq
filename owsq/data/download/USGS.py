import os,commands,urllib2
import ConfigParser #,logging
from cybercom.data.catalog import datacommons #catalog
#set catalog user and passwd
cfgfile = os.path.join(os.path.expanduser('/opt/celeryq'), '.cybercom')
config= ConfigParser.RawConfigParser()
config.read(cfgfile)
username = config.get('user','username')
password = config.get('user','password')


def save(name,path,query):
    temp=query
    temp.pop('source')
    return save_sitedata(name,path,temp)

def save_sitedata(name,path,query,data_provider='USGS-Tools-TypeSet'):
    #Load source web service data from metadata catalog
    dcommons = datacommons.toolkit(username,password)
    sources = dcommons.get_data('ows',{'spec':{'data_provider':data_provider}})[0]
    #Get Host information - NGINX root and urls from metadata catalog
    host=None
    hosts = dcommons.get_data('ows',{'spec':{'data_provider':'APP_HOSTS'},'fields':['sources']})[0]['sources']
    for item in(item for item in hosts if item['host']==os.uname()[1]):
        host=item
    for item in hosts:
        if item['host']==os.uname()[1]:
            host=item 
    if not host:
        raise 'No Host specified, Please upadate Catalog'
    sites=query['sites']
    params=query.copy()
    params['format']='rdb'
    params.pop('sites')
    metadata = sources[query['webservice_type']]
    params.pop('webservice_type')
    #metadata = sources[query['webservice_type']]
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
            #f1=open(os.path.join('/Users/mstacy',filename),'w')
            #print os.path.join('~',filename)
            f1=open(os.path.join(path,filename),'w')
            f1.write(res.read())
            #print os.path.join('~',filename)
            urlbase= host['base_directory']
            #urlbase='/Users/mstacy'
            print path.replace(urlbase ,host['url']) + filename + "YESSSSSS"
            return path.replace(urlbase ,host['url']) + filename
        except Exception as inst:
            print inst 
    return 'Bad Url'       
