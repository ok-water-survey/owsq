#Oklahoma Water Survey Queue Config
import os
import ConfigParser
#Catalog Set User and password
cfgfile = os.path.join(os.path.expanduser('/opt/celeryq'), '.cybercom')
configs = ConfigParser.RawConfigParser()
configs.read(cfgfile)
catalog_username = configs.get('user', 'username')
catalog_password = configs.get('user', 'password')


#mongo Connection
mongo_host = 'localhost'


#GeoJSON Utility Transformation setup
TARGET_PROJECTION = "+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +a=6378137 +b=6378137 +units=m +no_defs"
SOURCE_PROJECTION = "+proj=longlat +datum=NAD83 +no_defs"


#Mesonet Constants
mesonet_site_url = "http://www.mesonet.org/index.php/api/siteinfo/from_all_active_with_geo_fields/format/csv/"
mesonet_site_type = "csv"
mesonet_mongo_host = 'localhost'  #'fire.rccc.ou.edu'
mesonet_database = "ows"
mesonet_collection = "mesonet_site"

#WQP
wqp_url_template ="http://www.waterqualitydata.us/Result/search?siteid=%s&mimeType=csv"
#OWRB Constants
owrb_database = "ows"
owrb_welllog_collection = "owrb_well_logs"
well_logs_url = "http://static.cybercommons.org/OklahomaWaterSurvey/OWRB/geojson/Reported_Well_Logs.json"
#owrb_MonitorWells_collection = "owrb_monitoring_wells"
owrb_MonitorWells_collection = "owrb_water_wells"
#owrb sites
owrb_site_collection = "owrb_water_sites"
owrb_well_collection = "owrb_water_wells"
#USGS Constants
usgs_database = "ows"

# parameter
usgs_parameter_url = "http://nwis.waterdata.usgs.gov/usa/nwis/pmcodes?radio_pm_search=param_group&pm_group=All+--+include+all+parameter+groups&pm_search=&casrn_search=&srsname_search=&format=rdb&show=parameter_group_nm&show=parameter_nm&show=casrn&show=srsname&show=parameter_units"
usgs_parameter_collection = "parameters"

# site
usgs_site_url = "http://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=ok&siteStatus="
usgs_site_collection = "usgs_site"

#USGS webservice and metadata site template
usgs_site_metadata_url = "http://waterservices.usgs.gov/nwis/site/?format=rdb&sites=%s&seriesCatalogOutput=true"

#Oklahoma RTree spatial Database
watershed_collection = "watersheds"
aquifer_collection = "aquifers"

#water quality portal
wqp_result = "http://www.waterqualitydata.us/Result/search?siteid=%s&mimeType=csv"
wqp_site = "http://www.waterqualitydata.us/Station/search?statecode=40&mimeType=csv"
wqp_result_ok_all = "http://www.waterqualitydata.us/Result/search?statecode=40&mimeType=csv&zip=yes"
wqp_tmp = "/data/static/tmp"

#occ csv data
occ_site_collection = "occ_site"
