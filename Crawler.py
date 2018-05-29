#encoding: utf-8
import urllib
import urllib2
import json 
import time
import sqlite3
import os

try:
    import arcpy
except:
    pass
    

ak = "your ak"

DATABASEPATH = "temp.db"

def InitDatabase():
    if os.path.exists(DATABASEPATH):
        os.remove(DATABASEPATH)
    connect = sqlite3.connect(DATABASEPATH)

    connect.execute("CREATE TABLE poi(uid text primary key, name text, lat double, lng double, rank double)")
    connect.execute("CREATE TABLE boundary(uid text, lat double, lng double, orders integer, primary key (lat, lng, orders))")
    sql = "CREATE TABLE busline(busuid text primary key, name text)"
    connect.execute(sql)
    sql = "CREATE TABLE station(stationuid text primary key, name text, lng double, lat double)"
    connect.execute(sql)
    sql = "CREATE TABLE linestation(busuid text, stationuid text, buslineorder integer, primary key(busuid, stationuid, buslineorder))"
    connect.execute(sql)

    connect.commit()
    connect.close()

def insertPOI( POIList ):
    
    connect = sqlite3.connect(DATABASEPATH)
    
    for poi in POIList:
        cursor = connect.cursor()
        cursor.execute("SELECT * from poi where uid LIKE (:uids)", {"uids" : poi['uid'] })
        values= cursor.fetchall()
        if len(values) == 0:
            cursor.execute("INSERT INTO poi(uid, name, lat, lng)values(:uids, :names, :lats, :lngs)",
                {"uids" : poi['uid'], "names" : poi['name'], "lats" : poi['lat'], "lngs" : poi['lng'] })
            assert(cursor.rowcount == 1)
            boundary = poi['boundary']
            lens = len(boundary)

            for i in range(lens / 2):
                lng = boundary[2 * i]
                lat = boundary[2 * i + 1]
                sql = "INSERT INTO boundary(uid, lat, lng, orders)values(:uids, :lats, :lngs, :orders)"
                cursor.execute(sql, {
                    "uids" : poi['uid'],
                    "lats" : lat,
                    "lngs" : lng,
                    "orders" : i
                })
                assert(cursor.rowcount == 1)

    connect.commit()
    cursor.execute("SELECT * from poi")
    values = cursor.fetchall()
    print ( len(values) )
    cursor.close()
    connect.close()

def getHttp( url ):
    respon = urllib2.urlopen(url)
    ret = respon.read()
    ret = ret.decode('utf-8')
    return ret

def getBusUid( name ):
    url = "http://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&c=218&src=0&wd2=&pn=0&sug=0&l=11&from=webmap&biz_forward={%22scaler%22:1,%22styles%22:%22pl%22}&sug_forward=&tn=B_NORMAL_MAP&nn=0&wd=" + name.decode("gbk").encode("utf-8")
    #print ("url : %s" % (url) )
    respon = getHttp( url )
    retjson = json.loads(respon)
    try:
        contents = retjson['content']
    except Exception as e:
        print ( e )
        return []
    uids = []
    for item in contents:
        try:
            blinfo = item['blinfo']
            for info in blinfo:
                uids.append(info["uid"]);
        except:
            pass        

    s = set(uids)
    uids = [i for i in s]
    #print (uids)
    return uids

def getBusLine( uid ):
    url = "http://map.baidu.com/?qt=bsl&newmap=1&c=218&uid=" + uid
    respon = getHttp( url)
    retjson = json.loads(respon)
    try:
        lines = retjson['content'][0]

        busname = lines['name']
        busuid = uid 
        busstation = []
        station = lines['stations']

        for item in station:
            geo = item['geo']
            start = int(geo.find("1|")) + 2
            end = int( geo.find(";") )
            latlng = geo[start: end]
            latlng = latlng.split(",")
            lng = float(latlng[0])
            lat = float(latlng[1])
            stationuid = item['uid']
            stationname = item['name']
            coordinate = coordinateConvert( [lng, lat] )
            lng = coordinate[0]
            lat = coordinate[1]
            di = {
                "lat" : lat,
                "lng" : lng,
                "stationuid" : stationuid,
                "stationname" : stationname
            }
            busstation.append( di )
        return { "busname" : busname, "busuid" : busuid, "stations" : busstation}
    except:
        with open("log.txt", "w") as f:
            f.write(json.dumps(retjson))
        return None 

def insertBusLine( data ):
    
    if type( data) != dict:
        return False

    connect = sqlite3.connect(DATABASEPATH)
    cursor = connect.cursor()

    cursor.execute("SELECT * from busline where busuid = (:uid)", {"uid":data['busuid']})

    if len(cursor.fetchall()) <= 0:
        
        cursor.execute("INSERT INTO busline(busuid, name)values((:uid), (:name))", {"uid":data['busuid'], "name":data['busname']})
        assert(cursor.rowcount == 1)
    
        busstations = data['stations']
        
        for station in busstations:
            cursor.execute("SELECT * from station where stationuid = (:uid)", {"uid" : station['stationuid']})
            if len( cursor.fetchall() ) <= 0:
                cursor.execute("INSERT INTO station(stationuid, name, lat, lng)values((:uid), (:names), (:lat), (:lng))", {"uid":station['stationuid'], "names" : station['stationname'], "lat" : station['lat'], "lng" : station['lng']})
                assert(cursor.rowcount == 1)

        for i in range( len(busstations) ):
            station = busstations[i]
            cursor.execute("INSERT INTO linestation(busuid, stationuid, buslineorder)values((:busuid), (:stationuid), (:order))", {"busuid": data['busuid'], "stationuid" : station['stationuid'], "order" : i})
            assert(cursor.rowcount == 1)
        cursor.execute("SELECT * from busline")
        v = cursor.fetchall()
        #print ( len(v) )
    connect.commit()
    cursor.close()
    connect.close()
    return True

def coordinateConvert( coordinateList , froms = 6, to = 5):
    if (len(coordinateList) <= 0):
        return [] 
    coords = ""
    for i in range( len(coordinateList) / 2):
        lng = coordinateList[2 * i]
        lat = coordinateList[2 * i + 1]
        coords += str(lng) + "," + str(lat) + ";"
    coords = coords[:-2]

    url = "http://api.map.baidu.com/geoconv/v1/?from=%d&to=%d&ak=%s&coords=%s" % (froms, to, ak, coords)
    
    respon = getHttp(url)
    #print (respon)
    retjson = json.loads(respon)
    r = []
    if int(retjson['status'] == 0):
        result = retjson['result']

        for item in result:
            r.append(item['x'])
            r.append(item['y'])
    return r



def name2latlng(name):
    header = {
        "ak" : ak,
        "address" : name,
        "output" : "json"
    }

    url = "http://api.map.baidu.com/geocoder/v2/?"

    newurl = url + urllib.urlencode(header)
    
    print ("newurl : %s" % (newurl))

    respon = urllib2.urlopen(newurl)

    ret = respon.read()
    ret = ret.decode('utf-8')
    retjson = json.loads(ret)
    if retjson['status'] != 0:
        #print (retjson)
        return None
    else:
        return { "lat" : float(retjson['result']['location']['lat']),
                "lng" : float(retjson['result']['location']['lng'])
                }
    

def getBoundery(uid) : 
    url = "http://map.baidu.com/?ugc_type=3&ugc_ver=1&qt=detailConInfo&compat=1&t=1527301038989&uid=%s" % (str(uid))
    respon = urllib2.urlopen(url)
    ret = respon.read()
    ret = ret.decode('utf-8')

    retjson = json.loads(ret)
    
    try:
        geopoints = retjson['content']['ext']['detail_info']['guoke_geo']['geo']
        # parse geopoints and return a list
        # simple implement
        # if not work well, trun into RegExp
        start = geopoints.find("-")
        
        strs = geopoints[start + 1: -2]

        strs = strs.split(",")
        ret = map(float, strs)
        return list(ret)

    except:
        return []
        

  
def nearBySearchCity(cityName, pagenum = 0, pagesize = 20, keyword = "小区") : 
    locationKey = name2latlng(cityName)
    if not locationKey :
        return None

    nearBySearchHeader = {
        "query" : keyword,
        # MUST
        # the key word for query
         
        # "tag" : xxx,
        # option, the class of POI

        "location" : str(locationKey['lat']) + "," + str(locationKey['lng']), 
        # MUST
        # the center point of search
        
        "radius" : 10000, 
        # OPTION
        # Unit : meter
        # search radius

        #"radius_limit" : "false",
        # OPTION
        # whether are the all of result located in the search cycle

        "output" : "json",  
        # option, json | xml
        #"scope" : 1,  
         # option, 1 | 2 the detialed level of POI
        #"filter" : option, 
        # option
        #"coord_type" : 3,
        # option

        # "ret_coordtype" :sss,
        # option
        "page_size" : pagesize,
        # option return item size

        "page_num" : pagenum,
        # option
        
        "ak" : ak
        # must

        #"sn" : sn,
        # option

        #"timestamp" : d
        # option
    }

    req = urllib.urlencode(nearBySearchHeader)
    url = "http://api.map.baidu.com/place/v2/search"
    newurl = url + "?" + req

    
    response = urllib2.urlopen(newurl)
    ret = response.read().decode("utf-8")
    print (ret)

    retjson = json.loads(ret)
    poiList = []
    if int( retjson['status'] ) != 0 :
        return []
    else: 
        resultlist = retjson['results']
        for item in resultlist :
            #print (item)
            name = item['name']
            lat = float(item['location']['lat'])
            lng = float(item['location']['lng'])
            uid = item['uid']
            address = item['address']
            # there are still some of data
            # if need, please find detail in documents of BaiDu Map
            boundaryPolygon = getBoundery(uid)
            # TODO: unit convert
            boundaryPolygon = coordinateConvert(boundaryPolygon)
            if len(boundaryPolygon) != 0:
                poiList.append( { "name" : name,
                                    "lat" : lat,
                                    "lng" : lng,
                                    "uid" : uid,
                                    "boundary" : boundaryPolygon})
        return poiList
            #print (boundaryPolygon)
            # insert into database for cache
            # can insert into redis for cache
            # and for next step process

def checkSQL():
    connect = sqlite3.connect(DATABASEPATH)

    cursor = connect.cursor()

    cursor.execute("select * from busline")
    print (len(cursor.fetchall()))
    cursor.execute("select * from station")
    print (len(cursor.fetchall()))
    cursor.execute("select * from linestation")
    print (len(cursor.fetchall()))

    cursor.close()
    connect.close()

def updates():
    connect = sqlite3.connect(DATABASEPATH)

    cursor = connect.cursor()

    cursor.execute("SELECT stationuid, lng, lat, lngmeter, latmeter from station")
    values = cursor.fetchall()
    try:
        for row in values:
            uid = row[0]
            lng = row[1]
            lat = row[2]
            if float(row[3]) < 1e-9 and float(row[4]) < 1e-9:
                print ("lng = %s, lat = %s" % (lng, lat) )
                coordinate = coordinateConvert( [lng, lat], froms = 5, to = 6 )
                print ("coordinate[0] = %s, coordinate[1] = %s" % ( str(coordinate[0]), str(coordinate[1] )))
                cursor.execute("UPDATE station SET lngmeter = (:lng), latmeter = (:lat) where stationuid = (:uid)", {
                    "uid" : uid,
                    "lng" : coordinate[0],
                    "lat" : coordinate[1]
                })
    except Exception as e:
        print ( e )

    connect.commit()
    cursor.close()
    connect.close()

def updatesPOI():
    connect = sqlite3.connect(DATABASEPATH)

    cursor = connect.cursor()

    cursor.execute("SELECT uid, lng, lat, lngmeter, latmeter from poi")
    values = cursor.fetchall()
    count = 1
    try:
        for row in values:
            uid = row[0]
            lng = row[1]
            lat = row[2]
            if float(row[3]) < 1e-9 and float(row[4]) < 1e-9:
                print ("lng = %s, lat = %s" % (lng, lat) )
                coordinate = coordinateConvert( [lng, lat], froms = 5, to = 6 )
                print ("coordinate[0] = %s, coordinate[1] = %s" % ( str(coordinate[0]), str(coordinate[1] )))
                cursor.execute("UPDATE poi SET lngmeter = (:lng), latmeter = (:lat) where uid = (:uid)", {
                    "uid" : uid,
                    "lng" : coordinate[0],
                    "lat" : coordinate[1]
                })
    except:
        pass

    connect.commit()
    cursor.close()
    connect.close()

def Crawler(location):
    pass

if __name__ == "__main__":

    updates()
    updatesPOI()