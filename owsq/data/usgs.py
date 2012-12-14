import json,urllib2,StringIO,csv,ConfigParser,os
from celery.task import task
from celery.task.sets import subtask
from celery import chord
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
def usgs_get_sitedata(siteno):
    dcommons = datacommons.toolkit(username,password)
    records= dcommons.get_data(commons_name,{'spec':{'datasource':'USGS'},'fields':['sources']})
    return json.dumps(records)
