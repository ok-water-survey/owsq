from owsq import config
import geojson,pyproj
from shapely.geometry import Point
from shapely.geometry import shape

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
def intersect_point(objshape,lat,lon, t_srs=config.TARGET_PROJECTION, s_srs=config.SOURCE_PROJECTION):
    '''Transform point checks if Point intersects objshape '''
    x,y = transform_point(lat,lon,t_srs,s_srs)
    point = Point(x,y)
    if point.intersects(shape(objshape)):
        return True
    return False
