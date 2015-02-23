import os, urllib2  #,urllib
#import ConfigParser
#from cybercom.data.catalog import datacommons  #catalog
#from owsq.data.download import filezip
from subprocess import call
from celery.task import task
from owsq import config
from pymongo import MongoClient
#import dateutil.parser
#from pymongo import Connection
#from scrapy.selector import HtmlXPathSelector
#from BeautifulSoup import BeautifulSoup
#from datetime import datetime
#set catalog user and passwd
#username = config.catalog_username  #s.get('user','username')
#password = config.catalog_password  #s.get('user','password')

@task
def save(path, source, data_items=[]):  #name,path,query):
    '''Based function to all source imports in Download module'''
    db = MongoClient(config.catalog_uri)
    #dcommons = datacommons.toolkit(username, password)
    consol_data = consolidate(data_items)
    sourcepath = os.path.join(path, 'WaterQuality', 'data')
    call(['mkdir', '-p', sourcepath])
    urls = []
    host = get_host(db) #commons)
    urlbase = host['base_directory']
    url_template = config.wqp_url_template
    for key, value in consol_data.items():
        page = urllib2.urlopen(url_template % (value['query']['site_no']))
        filename = 'WaterQuality_%s.csv' % (value['query']['site_no'])
        f1 = open(os.path.join(sourcepath, filename), 'w')
        f1.write(page.read())
        f1.close()
        urls.append(os.path.join(sourcepath.replace(urlbase, host['url']), filename))
    return urls


def consolidate(data_items):
    cons_queries = {}
    for item in data_items:
        node = "%s" % (item['query']['site_no'])
        cons_queries[node] = item
        #if node not in cons_queries:
        ##    cons_queries[node] = item
        ##else:
        #    if cons_queries[node]['query']['startDT'] > item['query']['startDT']:
        #        cons_queries[node]['query']['startDT'] = item['query']['startDT']
        #    if cons_queries[node]['query']['endDT'] < item['query']['endDT']:
        #        cons_queries[node]['query']['endDT'] = item['query']['endDT']
    return cons_queries


def get_host(db):
    hosts = db['ows']['data'].find({'data_provider':'APP_HOSTS'},fields=['sources'])[0]['sources']
    #hosts = dcommons.get_data('ows', {'spec': {'data_provider': 'APP_HOSTS'}, 'fields': ['sources']})[0]['sources']
    for item in (item for item in hosts if item['host'] == os.uname()[1]):
        return item
    raise 'No Host specified, Please upadate Catalog'
