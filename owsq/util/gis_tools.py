from owsq import config
import geojson,pyproj
from shapely.geometry import Point
from shapely.geometry import shape
from pymongo import Connection
#from shapely.geometry import shape
from rtree import index

def mkGeoJSONPoint(obj,latkey,lonkey,attributes=False, transform=False, t_srs=config.TARGET_PROJECTION, s_srs=config.SOURCE_PROJECTION):
    ''' Return geojson feature collection ''' 
    FC = []
    for item in obj:
        x = item.pop(lonkey)
        y = item.pop(latkey)
        if transform:
            p1 = pyproj.Proj(s_srs)
            p2 = pyproj.Proj(t_srs)
            x, y = pyproj.transform( p1, p2, x, y)
        if attributes:
            FC.append( geojson.Feature( geometry=geojson.Point((x,y)), properties = item ) )
        else:
            FC.append( geojson.Feature( geometry=geojson.Point((x,y)) ) )
    return geojson.dumps(geojson.FeatureCollection( FC ), indent=2)

def transform_point(lat,lon, t_srs=config.TARGET_PROJECTION, s_srs=config.SOURCE_PROJECTION):
    '''Transform point from source to project projection '''
    p1 = pyproj.Proj(s_srs)
    p2 = pyproj.Proj(t_srs)
    x, y = pyproj.transform(p1, p2,lon,lat)
    return x,y
def intersect_point(objshape,lat,lon, t_srs=config.TARGET_PROJECTION, s_srs=config.SOURCE_PROJECTION,transform=True):
    '''Transform point checks if Point intersects objshape '''
    if transform==True:
        x,y = transform_point(lat,lon,t_srs,s_srs)
    else:
        x,y = lat,lon
    point = Point(x,y)
    if point.intersects(shape(objshape)):
        return True
    return False

def ok_watershed_aquifer_rtree(database='ows',watershed_collection=config.watershed_collection,aquifer_collection=config.aquifer_collection):
    """ 
        Returns rtree spatial index of bounds of Oklahoma watersheds and aquifers.
        returns rtree index and dictionary corresponding to rtree index as the key with type and properties
    """
    db = Connection(config.mongo_host)
    data={}
    indx=1
    idx = index.Index()
    for geo in db[database][watershed_collection].find():
        obj = shape(geo['geometry'])
        idx.insert(indx, obj.bounds, obj=obj)
        data[indx] = {'shape':obj,'properties': geo['properties'],'type':'watershed'}
        indx +=1
    for geo in db[database][aquifer_collection].find():
        obj = shape(geo['geometry'])
        idx.insert(indx, obj.bounds, obj=obj)
        data[indx] = {'shape':obj,'properties': geo['properties'],'type':'aquifer'}
        indx +=1
    return idx,data
    
    
