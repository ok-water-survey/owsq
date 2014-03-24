from owsq import config
import geojson, pyproj
from shapely.geometry import Point
from shapely.geometry import shape
from pymongo import Connection
#from shapely.geometry import shape
from rtree import index
import zipfile, os.path
import urllib2

def mkGeoJSONPoint(obj, latkey, lonkey, attributes=False, transform=False, t_srs=config.TARGET_PROJECTION,
                   s_srs=config.SOURCE_PROJECTION):
    ''' Return geojson feature collection '''
    FC = []
    for item in obj:
        x = item.pop(lonkey)
        y = item.pop(latkey)
        if transform:
            p1 = pyproj.Proj(s_srs)
            p2 = pyproj.Proj(t_srs)
            x, y = pyproj.transform(p1, p2, x, y)
        if attributes:
            FC.append(geojson.Feature(geometry=geojson.Point((x, y)), properties=item))
        else:
            FC.append(geojson.Feature(geometry=geojson.Point((x, y))))
    return geojson.dumps(geojson.FeatureCollection(FC), indent=2)


def transform_point(lat, lon, t_srs=config.TARGET_PROJECTION, s_srs=config.SOURCE_PROJECTION):
    '''Transform point from source to project projection '''
    p1 = pyproj.Proj(s_srs)
    p2 = pyproj.Proj(t_srs)
    x, y = pyproj.transform(p1, p2, lon, lat)
    return x, y


def intersect_point(objshape, lat, lon, t_srs=config.TARGET_PROJECTION, s_srs=config.SOURCE_PROJECTION, transform=True):
    '''Transform point checks if Point intersects objshape '''
    if transform == True:
        x, y = transform_point(lat, lon, t_srs, s_srs)
    else:
        x, y = lat, lon
    point = Point(x, y)
    if point.intersects(shape(objshape)):
        return True
    return False


def ok_watershed_aquifer_rtree(database='ows', watershed_collection=config.watershed_collection,
                               aquifer_collection=config.aquifer_collection):
    """ 
        Returns rtree spatial index of bounds of Oklahoma watersheds and aquifers.
        returns rtree index and dictionary corresponding to rtree index as the key with type and properties
    """
    db = Connection(config.mongo_host)
    data = {}
    indx = 1
    idx = index.Index()
    for geo in db[database][watershed_collection].find():
        obj = shape(geo['geometry'])
        idx.insert(indx, obj.bounds, obj=obj)
        data[indx] = {'shape': obj, 'properties': geo['properties'], 'type': 'watershed'}
        indx += 1
    for geo in db[database][aquifer_collection].find():
        obj = shape(geo['geometry'])
        idx.insert(indx, obj.bounds, obj=obj)
        data[indx] = {'shape': obj, 'properties': geo['properties'], 'type': 'aquifer'}
        indx += 1
    return idx, data


def save_download(url, location, compress=None):
    """
     Download hadles regular text files and zip. Will add tar in the future.
    """
    download = urllib2.urlopen(url)
    if compress == 'zip':
        output = open(location, 'wb')
        output.write(download.read())
        output.close()
        unzip(location, os.path.split(os.path.abspath(location))[0])
        os.remove(location)
        return os.path.split(os.path.abspath(location))[0]
    else:
        output = open(location, 'w')
        output.write(download.read())
        output.close()
        return location


def unzip(source_filename, dest_dir):
    zf = zipfile.ZipFile(source_filename)
    zf.extractall(path=dest_dir)
        #for member in zf.infolist():
            # Path traversal defense copied from
            # http://hg.python.org/cpython/file/tip/Lib/http/server.py#l789
        #    words = member.filename.split('/')
        #    path = dest_dir
        #    for word in words[:-1]:
        #        drive, word = os.path.splitdrive(word)
        #        head, word = os.path.split(word)
        #        if word in (os.curdir, os.pardir, ''): continue
        #        path = os.path.join(path, word)
        #    zf.extract(member, path)


    
    
