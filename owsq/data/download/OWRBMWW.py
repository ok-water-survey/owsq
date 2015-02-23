import os, urllib2  #,urllib
#import ConfigParser
#from cybercom.data.catalog import datacommons  #catalog
#from owsq.data.download import filezip
from subprocess import call
from celery.task import task
from owsq import config
import dateutil.parser
from pymongo import Connection
from scrapy.selector import HtmlXPathSelector
from BeautifulSoup import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
#set catalog user and passwd
#username = config.catalog_username  #s.get('user','username')
#password = config.catalog_password  #s.get('user','password')


@task
def save(path, source, data_items=[]):  #name,path,query):
    '''Based function to all source imports in Download module'''
    db1 = MongoClient(config.catalog_uri)
    #dcommons = datacommons.toolkit(username, password)
    consol_data = consolidate(data_items)
    sourcepath = os.path.join(path, 'OWRB', 'Monitor_Wells')
    call(['mkdir', '-p', sourcepath])
    urls = []
    database = config.owrb_database
    collection = config.owrb_well_collection
    host = get_host(db1)#commons)
    urlbase = host['base_directory']
    meso_url = "http://www.mesonet.org/index.php/meteogram/data/owrb_text//stid/%s/year/%s/month/%s/day/%s/timelen/%sd/product/GH20/type/csv"
    db = Connection(config.mongo_host)
    for key, value in consol_data.items():
        if value['query']['webservice_type'] == 'mesonet':
            filename = 'OWRB_MonitoringWell_mesonet%s.csv' % (value['query']['sites'])
            sitedata = db[database]['owrb_monitor_sites'].find_one({'WELL_ID': value['query']['sites']})
            mesosite = sitedata['mesonetID']
            start = dateutil.parser.parse(value['query']['startDT'])
            end = dateutil.parser.parse(value['query']['endDT'])
            day_count = (end - start).days + 1
            month = end.strftime("%m")
            year = end.strftime("%Y")
            day = end.strftime("%d")
            url = meso_url % (mesosite, year, month, day, day_count)

            f1 = open(os.path.join(sourcepath, filename), 'w')
            res = urllib2.urlopen(url)
            f1.write(res.read())
            f1.close()
            urls.append(os.path.join(sourcepath.replace(urlbase, host['url']), filename))
        else:
            filename = 'OWRB_MonitoringWell_%s.csv' % (value['query']['sites'])
            f1 = open(os.path.join(sourcepath, filename), 'w')
            head = "site,date,measurement,unit,status,project\n"
            f1.write(head)
            #print head
            temp_tmpl = "%s,%s,%s,%s,%s,%s\n"
            for row in db[database][collection].find({'site': value['query']['sites']}).sort([('sort_date', -1), ]):
                temp = temp_tmpl % (
                row['site'], row['observed_date'], row['value'], row['unit'], row['status'], row['project'])
                f1.write(temp)
            f1.close()
            urls.append(os.path.join(sourcepath.replace(urlbase, host['url']), filename))
    return urls


def update_data(site_id):
    db=Connection('worker.oklahomawatersurvey.org')
    cols = {'1': 'observed_date', '2': 'value', '3': 'status', '4': 'project'}
    now = datetime.now()
    url_template = 'http://www.owrb.ok.gov/wd/search_test/water_levels.php?siteid=%s'
    response = urllib2.urlopen(url_template % site_id)
    hxs = HtmlXPathSelector(response)
    tables = hxs.select('//tr')
    for tab in tables:
        data = {'site': site_id, 'source': response.url, 'unit': 'ft', 'unit_description': 'feet from land surface',
                'scrape_date': now}
        soup = BeautifulSoup(tab.extract().encode("ascii", "ignore"))
        i = 1
        for col in soup.findAll(name='td'):
            try:
                if i == 1:
                    temp = col.contents[0].strip(' \t\n\r')
                    data['sort_date'] = datetime.strptime(temp, '%m/%d/%Y %I:%M %p')
                data[cols[str(i)]] = col.contents[0].strip(' \t\n\r')
                i += 1
            except:
                print data
        #print data
        if len(data.keys()) > 5:
            db.ows.owrb_monitoring_wells_test.save(data)


def consolidate(data_items):
    cons_queries = {}
    for item in data_items:
        node = "%s_%s" % (item['query']['sites'], item['query']['webservice_type'])
        if node not in cons_queries:
            cons_queries[node] = item
        else:
            if cons_queries[node]['query']['startDT'] > item['query']['startDT']:
                cons_queries[node]['query']['startDT'] = item['query']['startDT']
            if cons_queries[node]['query']['endDT'] < item['query']['endDT']:
                cons_queries[node]['query']['endDT'] = item['query']['endDT']
    return cons_queries


def get_host(db): #commons):
    hosts = db['ows']['data'].find({'data_provider':'APP_HOSTS'},fields=['sources'])[0]['sources']
    #hosts = dcommons.get_data('ows', {'spec': {'data_provider': 'APP_HOSTS'}, 'fields': ['sources']})[0]['sources']
    for item in (item for item in hosts if item['host'] == os.uname()[1]):
        return item
    raise 'No Host specified, Please upadate Catalog'
