#encoding:utf-8
import sqlite3
import convert2shape
import Crawler
import numpy as np
import math
DATABASEPATH = "temp.db"
SHAPEPATH = "./project/test.shp"
def importPolygon():

    convert2shape.createFeature(SHAPEPATH, ["uid", "name"], ["TEXT", "TEXt"])

    connect = sqlite3.connect(DATABASEPATH)

    cursor = connect.cursor()
    
    sql = "SELECT uid, name from poi"
    
    cursor.execute(sql)

    values = cursor.fetchall()

    for row in values:

        uid = row[0]
        name = row[1]
        sql = "SELECT lng, lat, orders from boundary where uid=(:uids) order by orders"
        cursor.execute(sql, {"uids" : uid})
        values = cursor.fetchall()
        points = []
        for point in values:
            points.append( (point[0], point[1]) )

        print (points)
        points = convert2shape.constructPolygon(points)
        convert2shape.insertRow(SHAPEPATH, ("uid", "name", "SHAPE@"), [uid, name, points])

def importPoint():
    convert2shape.createFeature("./project/point.shp", ["uid", "name", "lng", "lat"], ["TEXT", "TEXT", "DOUBLE", "DOUBLE"])

    connect = sqlite3.connect(DATABASEPATH)

    cursor = connect.cursor()
    sql = "SELECT stationuid, name, lng, lat from station"
    cursor.execute(sql)
    values = cursor.fetchall()

    for row in values:
        uid = row[0]
        name = row[1]
        lng = float(row[2])
        lat = float(row[3])
        point = convert2shape.constructPoint(lng, lat)
        convert2shape.insertRow("./project/point.shp", ("uid", "name", "lng", "lat", "SHAPE@"), [uid, name, lng, lat, point])

def importPolyline():
    path = "./porject/polyline.shp"
    convert2shape.createFeature(path, ["uid", "name"], ["TEXT", "TEXt"])

    connect = sqlite3.connect(DATABASEPATH)

    cursor = connect.cursor()
    sql = "SELECT busuid, name from busline"
    cursor.execute(sql)
    if cursor.rowcount != 0:
        values = cursor.fetchall()

        for row in values:
            busuid = row[0]
            busname = row[1]

            points = []
            sql = "SELECT stationuid, buslineorder from buslineorder where busuid = (:uid) order by buslineorder"
            cursor.execute(sql,{"uid" : busuid})
            assert(cursor.rowcount != 0)
            linestations = cursor.fetchall()

            for singlepoint in linestations:
                sql = "SELECT lng, lat from station where stationuid = (:uid)"
                cursor.execute(sql, {"uid" : singlepoint[0]})
                assert(cursor.rowcount == 1)
                station = cursor.fetchall()
                point = station[0]
                points.append( (point[0], point[1]) )

            lines = convert2shape.constructPolyline( points )
            convert2shape.insertRow(path, ["uid", "name", "SHAPE@"], [busuid, busname, lines])


def PageRank():

    connect = sqlite3.connect(DATABASEPATH)

    cursor = connect.cursor()

    cursor.execute("SELECT stationuid from station")
    resultset = cursor.fetchall()

    StationUID = [row[0] for row in resultset]
    N = len(StationUID)
    d = 0.3
    M = np.zeros( (N, N) , dtype = "float")
    R = np.ones( (N, 1), dtype = "float") * (1.0 - d) / N
    bias = np.ones( (N, 1) , dtype = "float" ) * (1.0 - d) / N

    for i in range(N):
        for j in range(i + 1, N):
            
            cursor.execute("SELECT busuid from linestation where stationuid = (:stationuid)", {"stationuid" : StationUID[i]})
            
            resultset1 = cursor.fetchall()

            for row in resultset1:
                busuid = row[0]

                cursor.execute("SELECT * from linestation where busuid = (:busuid), stationuid = (:stationuid)", {"busuid" : busuid, "stationuid" : StationUID[j]})

                if len(cursor.fetchall()) == 1:
                    cursor.execute("SELECT count(*) from linestation where busuid = (:busuid)", {"busuid" : busuid})
                    L = cursor.fetchone()
                    M[i, j] = M[j, i] = 1.0 / float(L[0])
                    
    maxEpoch = 100
    epsilon = 1e-6
    for epoch in range(maxEpoch):
        last_R = R
        R = d * M.dot(R) + bias

        if  np.sum( np.sum( np.power(R - last_R, 2) ) ) < N**2 * epsilon:
            break 

    cursor.close()
    connect.close()

    return [{"stationuid" : StationUID[i], "rank" : R[i]} for i in range(N)]


def distanceDelay( x ):
    if x < 0 :
        return 0.0

    if x >= 0 and x <= 400:
        return 1.0

    if x <= 1600:
        return -153.6558 * x**3 + 419.4604 * x**2 - 395.9706 * x + 201.1086
    
    return -92.8 * x**3 + 566.6 * x**2 -1153.1 * x + 786.6

# to gcj02 project
def baiduProj( lng, lat ):
    return Crawler.coordinateConvert( (lng, lat), froms = 5, to = 3)

def distance( lng1, lat1, lng2, lat2, proj = baiduProj):

    point1 = proj( lng1, lat1)
    point2 = proj( lng2, lat2)

    return math.sqrt( (point1[0] - point2[0])**2 + (point1[1] - point2[1])**2)

def accessibility():
    connect = sqlite3.connect(DATABASEPATH)

    cursor = connect.cursor()

    cursor.execute("SELECT uid, lat, lng from poi")

    resultset = cursor.fetchall()

    pois = [{"uid" : row[0], "lat" : row[1], "lng" : row[2]} for row in resultset]
    Weight = PageRank()

    distanceMatrix = np.zeros( (len(pois), len(Weight)) , dtype = "float")
    weightdelay = np.zeros( (len(pois), len(Weight)) , dtype = "float")

    for i in range( len(pois) ):
        for j in range( len(Weight) ):
            stationuid = Weight[j]['stationuid']
            cursor.execute("SELECT lng, lat from station where stationuid = (:uid)", {"uid" : stationuid})
            result = cursor.fetchone()
            lng = result[0]
            lat = result[1]

            distanceMatrix[i, j] = distance( pois[i]['lng'], pois[i]['lat'], lng, lat)
            weightdelay[i, j] = distanceDelay( distanceMatrix[i, j])

    WeightVector = np.zeros( (len(Weight), 1), dtype = "float")

    for i in range(len(Weight)):
        WeightVector[i, 0] = Weight['rank'] 

    accessibilitys = weightdelay.dot(WeightVector)

    for i in range( len(pois) ):
        cursor.execute("UPDATE poi SET accessibility = (:accessibility) where uid = (:uid)", {"accessibility" : accessibilitys[i], "uid" : pois[i]['uid']})

    connect.commit()
    cursor.close()
    connect.close()
                    

    
    


    

if __name__ == "__main__" :
    #Crawler.InitDatabase()
    #for i in range(10):
    #    r = Crawler.nearBySearchCity("武汉大学", pagesize = 100, pagenum = i)
    #    Crawler.insertPOI(r)
    
    #importPolygon()
    #importPoint()

    #uids = getBusUid("72路")
    #for i in uids:
    #    data = getBusLine(i)
    #    insertBusLine(data)

    
    #checkSQL()