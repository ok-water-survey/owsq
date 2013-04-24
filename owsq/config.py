#Oklahoma Water Survey Queue Config

#GeoJSON Utility Transformation setup
TARGET_PROJECTION="+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +a=6378137 +b=6378137 +units=m +no_defs"
SOURCE_PROJECTION="+proj=longlat +datum=NAD83 +no_defs"


#Mesonet Constants
mesonet_site_url="http://www.mesonet.org/index.php/api/siteinfo/from_all_active_with_geo_fields/format/csv/"
mesonet_site_type="csv"
mesonet_mongo_host='localhost'#'fire.rccc.ou.edu'
mesonet_database="ows"
mesonet_collection="mesonet_site"
