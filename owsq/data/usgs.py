import json,urllib2,StringIO,csv,ConfigParser,os,commands
from celery.task import task
from celery.task.sets import subtask
#from celery import chord
from pymongo import Connection
from datetime import datetime,timedelta
from cybercom.data.catalog import datacommons #catalog
#set catalog user and passwd
cfgfile = os.path.join(os.path.expanduser('/opt/celeryq'), '.cybercom')
config= ConfigParser.RawConfigParser()
config.read(cfgfile)
username = config.get('user','username')
password = config.get('user','password')

mongoHost = 'localhost'
site_database='ows'

@task()
def load_site_metadata(site=None,site_collection='usgs_site',database=site_database,collection='usgs_site_metadata',delete=True):
    if site:
        sites= site.split(',')
        for s in sites:
            get_site_metadata(s,db,database,collection)
    else:
        db=Connection(mongoHost)
        if delete:
            db[database][collection].remove()
        sites= db[database][site_collection].find(timeout=False)#.limit(10)
        for site in sites:
            get_site_metadata(site['site_no'],db,database,collection)

def get_site_metadata(site,db,database,collection):#,database=site_database,collection='usgs_site_metadata',delete=True):
    '''
        Docstring
    '''
    url='http://waterservices.usgs.gov/nwis/site/?format=rdb&sites=%s&seriesCatalogOutput=true' % (site)
    f1 = urllib2.urlopen(url)
    temp='#'
    head=''
    while (temp[0]=="#"):
        temp=f1.readline()
        if temp[0]!='#':
            head = temp.strip('\r\n').split('\t')
    f1.readline()
    for row in f1:
        temp=row.strip('\r\n').split('\t')
        data = dict(zip(head,temp))
        try:
            param=db[database]['parameters'].find_one({'parameter_cd': data['parm_cd']})
            data['parameter']={'group_name':param['parameter_group_nm'],'name':param['parameter_nm'],'units':param['parameter_units']}
        except:
            pass
        exist=db[database][collection].find_one({'site_no':site,'parm_cd':data['parm_cd'],'srs_id':data['srs_id'],'data_type_cd':data['data_type_cd']})
        if exist:
            data['_id']=exist['_id']
        db[database][collection].save(data)
    #print head
@task()
def get_metadata_site(site,ws_url='http://waterservices.usgs.gov/nwis/site/?format=rdb&sites=%s&seriesCatalogOutput=true',database=site_database):
    db=Connection(mongoHost)
    url= ws_url % (site)
    f1 = urllib2.urlopen(url)
    temp='#'
    head=''
    output=[]
    while (temp[0]=="#"):
        temp=f1.readline()
        if temp[0]!='#':
            head = temp.strip('\r\n').split('\t')
    f1.readline()
    for row in f1:
        temp=row.strip('\r\n').split('\t')
        data = dict(zip(head,temp))
        try:
            param=db[database]['parameters'].find_one({'parameter_cd': data['parm_cd']})
            data['parameter']={'group_name':param['parameter_group_nm'],'name':param['parameter_nm'],'units':param['parameter_units']}
        except:
            pass
        output.append(data)
    return json.dumps(output)#, indent=2)
@task()
def usgs_sync(source,database=site_database):
    ''' 
        source - [usgs-wq,usgs,params,usgs-iv]

    '''
    if source == 'usgs-wq':
        return sites_usgs_wq(database,'usgs_wq_site')
    elif source == 'usgs':
        return sites_usgs(database,'usgs_site')
    elif source == 'params':
        return usgs_parameters(database,'parameters')
    elif source == 'usgs-iv':
        return usgs_iv(database,'usgs_iv_site')
    else:
        return json.dumps({'error': "Unknown source", 'available_sources': ['usgs','usgs-wq','usgs-iv','usgs-params']}, indent=2)

@task()
def sites_usgs(database=site_database,collection='usgs_site',delete=True):
    '''
        Task to update USGS 
        Sites
    '''
    db=Connection(mongoHost)
    if delete:
        db[database][collection].remove()
    url='http://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=ok&siteStatus=' 
    f1_i =urllib2.urlopen(url+'inactive') 
    f2_a=urllib2.urlopen(url+'active')
    f1_in=StringIO.StringIO(f1_i.read())
    f2_act=StringIO.StringIO(f2_a.read())
    temp='#'
    head=''
    while (temp[0]=="#"):
        temp=f2_act.readline()
        f1_in.readline()
        if temp[0]!='#':
            head = temp.strip('\r\n').split('\t') 
    head.append('status')
    f2_act.readline()
    f1_in.readline()
    for row in f2_act:
        temp = row.strip('\r\n').split('\t')
        temp.append('Active')
        db[database][collection].insert(dict(zip(head,temp)))
    for row in f1_in:
        temp = row.strip('\r\n').split('\t')
        temp.append('Inactive')
        db[database][collection].insert(dict(zip(head,temp)))
    return json.dumps({'source':'usgs','url':[url+'inactive',url+'active'],'database':database,'collection':collection,'record_count':db[database][collection].count()}, indent=2)
@task()
def sites_usgs_wq(database=site_database,collection='usgs_wq_site',delete=True):
    '''
        Task to update USEPA and USGS 
        Water Quality sites
    '''
    db=Connection(mongoHost)
    if delete:
        db[database][collection].remove()
    url = 'http://www.waterqualitydata.us/Station/search?statecode=40&mimeType=csv' 
    sites=urllib2.urlopen(url)
    output= StringIO.StringIO(sites.read())
    head=output.readline()
    head= head.replace('/','-').strip('\r\n').split(',')
    #print head
    reader = csv.DictReader(output,head)
    for row in reader:
        db[database][collection].insert(row)
    return json.dumps({'source':'usgs_wq','url':url,'database':database,'collection':collection}, indent=2)
@task()
def usgs_iv(database=site_database,collection='usgs_iv_site',delete=True):
    ''' Task to update USGS Sites
        Instantaneous Values Service for real-time data and historical data since October 1, 2007
     '''
    db=Connection(mongoHost)
    if delete:
        db[database][collection].remove()
    url='http://waterservices.usgs.gov/nwis/iv/?format=json&stateCd=ok'
    sites=urllib2.urlopen(url)
    data= json.loads(sites.read())
    uid=[]
    for row in data['value']['timeSeries']:
        if not row['sourceInfo']['siteCode'][0]['value'] in uid:
            uid.append(row['sourceInfo']['siteCode'][0]['value'])
            temp=row['sourceInfo']
            temp['site_no']=row['sourceInfo']['siteCode'][0]['value']
            db[database][collection].insert(temp)
    return json.dumps({'source':'usgs_iv','url':url,'database':database,'collection':collection}, indent=2)
@task()
def usgs_parameters(database=site_database,collection='parameters',delete=True):
    ''' Task to update USGS Parameters '''
    db=Connection(mongoHost)
    if delete:
        db[database][collection].remove()
    url='http://nwis.waterdata.usgs.gov/usa/nwis/pmcodes?radio_pm_search=param_group&pm_group=All+--+include+all+parameter+groups&pm_search=&casrn_search=&srsname_search=&format=rdb&show=parameter_group_nm&show=parameter_nm&show=casrn&show=srsname&show=parameter_units'
    param=urllib2.urlopen(url)
    output= StringIO.StringIO(param.read())
    temp='#'
    head=''
    while (temp[0]=="#"):
        temp=output.readline()
        if temp[0]!='#':
            head = temp.strip('\r\n').split('\t')    
            output.readline()
    for row in output:
        temp=row.strip('\r\n').split('\t')
        db[database][collection].insert(dict(zip(head,temp)))
    return json.dumps({'source':'params','url':url,'database':database,'collection':collection}, indent=2)
@task()
def usgs_get_sitedata(sites,type='instantaneous',params="{'format':'json'}",data_provider='USGS'):
    dcommons = datacommons.toolkit(username,password)
    records= dcommons.get_data('ows',{'spec':{'data_provider':data_provider},'fields':['sources']})
    sources = records[0]['sources']
    result={}
    for source,val in sources.items():
        #src_url.append(val['url'])
        #for url in src_url:
        temp=''
        if source==type:
            for k,v in json.loads(params.replace("'",'"')).items():
                temp= k + "=" + v + '&'
            
            url =val['url'] + temp + 'sites=' + sites 
            #print url
            urlcheck = commands.getoutput("wget --spider '" + url + "' 2>&1| grep 'Remote file exists'")
            if urlcheck:
                try:
                    res=urllib2.urlopen(url)
                    data= json.loads(res.read())
                    result[source]={'url':url,'data':data}
                except:
                    pass
                return json.dumps( result, indent=2 )


