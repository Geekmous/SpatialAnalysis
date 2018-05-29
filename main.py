#encoding:utf-8
import sqlite3
import convert2shape
import Crawler
import numpy as np
import math
import time
DATABASEPATH = "temp.db"
SHAPEPATH = "./project/test.shp"
def importPolygon():

    convert2shape.createFeature(SHAPEPATH, ["uid", "name", "rank"], ["TEXT", "TEXT", "DOUBLE"])

    connect = sqlite3.connect(DATABASEPATH)

    cursor = connect.cursor()
    
    sql = "SELECT uid, name, rank from poi"
    
    cursor.execute(sql)

    values = cursor.fetchall()

    for row in values:

        uid = row[0]
        name = row[1]
        rank = float(row[2])
        sql = "SELECT lng, lat, orders from boundary where uid=(:uids) order by orders"
        cursor.execute(sql, {"uids" : uid})
        values = cursor.fetchall()
        points = []
        for point in values:
            points.append( (point[0], point[1]) )

        #print (points)
        points = convert2shape.constructPolygon(points)
        
        convert2shape.insertRow(SHAPEPATH, ("uid", "name","rank", "SHAPE@"), [uid, name, rank, points])


def importPoint():
    convert2shape.createFeature("./project/point.shp", ["uid", "name", "lng", "lat"], ["TEXT", "TEXT", "DOUBLE", "DOUBLE"], featureTypes= "POINT")

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

        convert2shape.insertRow("./project/point.shp", ("uid", "name", "lng", "lat", "SHAPE@XY"), [uid, name, lng, lat, point])

def importPolyline():
    path = "./project/polyline.shp"
    convert2shape.createFeature(path, ["uid", "name"], ["TEXT", "TEXT"], featureTypes= "POLYLINE")

    connect = sqlite3.connect(DATABASEPATH)

    cursor = connect.cursor()
    sql = "SELECT busuid, name from busline"
    cursor.execute(sql)
    if 1:
        values = cursor.fetchall()

        for row in values:
            busuid = row[0]
            busname = row[1]

            points = []
            sql = "SELECT stationuid, buslineorder from linestation where busuid = (:uid) order by buslineorder"
            cursor.execute(sql,{"uid" : busuid})
            
            linestations = cursor.fetchall()

            for singlepoint in linestations:
                sql = "SELECT lng, lat from station where stationuid = (:uid)"
                cursor.execute(sql, {"uid" : singlepoint[0]})
                
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

    print ("Start Construct Data.")

    stationUID2busUID = {}
    for i in range(N):
            cursor.execute("SELECT busuid from linestation where stationuid = (:stationuid)", {"stationuid" : StationUID[i]})
            resultset = cursor.fetchall()
            r = [row[0] for row in resultset]
            stationUID2busUID[StationUID[i]] = r
    
    linestationSize = {}

    cursor.execute("SELECT busuid from busline")
    resultset = cursor.fetchall()

    for row in resultset:
        busuid = row[0]
        cursor.execute("SELECT count(*) from linestation where busuid = (:busuid)", {"busuid" : busuid})
        sizes = cursor.fetchone()[0]
        linestationSize[busuid] = sizes

    print ("Start Construct Matrix.")
    for i in range(N):
        for j in range(i + 1, N):
            
            resultset1 = stationUID2busUID[StationUID[i]]

            set1 = set(resultset1)

            resultset2 = stationUID2busUID[ StationUID[j] ]
            set2 = set(resultset2)

            set3 = set1 & set2

            if len(set3) > 0:
                M[i, j] = M[j, i] = 0
                for busUID in set3:
                    s = linestationSize[busUID]
                    M[i, j] += 1.0 / s
                    M[j, i] += 1.0 / s

        print ("...%f %%" % ( 100.0 * i / N) )

                    
    maxEpoch = 100
    epsilon = 1e-9

    for epoch in range(maxEpoch):
        start_time = time.time()
        last_R = R
        R = d * M.dot(R) + bias
        print ( np.sum(np.sum( np.power(R - last_R, 2))))
        end_time = time.time()
        print ("epoch %d in %d : %f s" % ( epoch + 1, maxEpoch, end_time - start_time) )
        if  np.sum( np.sum( np.sqrt (np.power(R - last_R, 2) ) ) ) < epsilon:
            break 
    print ("Page Rank done")
    cursor.close()
    connect.close()

    return [{"stationuid" : StationUID[i], "rank" : R[i]} for i in range(N)]


def distanceDelay( x ):
    if x < 0 :
        return 0.0

    if x >= 0 and x <= 400:
        return 1.0

    if x <= 1600:
        x = 1.0 * x / 1000
        return (-153.6558 * x**3 + 419.4604 * x**2 - 395.9706 * x + 201.1086) / 100
    
    if x <= 2400:
        x = 1.0 * x / 1000
        return (-92.8 * x**3 + 566.6 * x**2 -1153.1 * x + 786.6) / 100
    return 0

# to gcj02 project
def baiduProj( lng, lat ):
    return Crawler.coordinateConvert( (lng, lat), froms = 5, to = 3)

def distance( lng1, lat1, lng2, lat2, proj = None):
    if proj == None:
        point1 = [lng1, lat1]
        point2 = [lng2, lat2]
    else:
        point1 = proj( lng1, lat1 )
        point2 = proj( lng2, lat2 )

    return math.sqrt( (point1[0] - point2[0])**2 + (point1[1] - point2[1])**2)

def accessibility():
    connect = sqlite3.connect(DATABASEPATH)

    cursor = connect.cursor()

    cursor.execute("SELECT uid, latmeter, lngmeter from poi")

    resultset = cursor.fetchall()

    pois = [{"uid" : row[0], "latmeter" : row[1], "lngmeter" : row[2]} for row in resultset]
    Weight = PageRank()

    distanceMatrix = np.zeros( (len(pois), len(Weight)) , dtype = "float")
    weightdelay = np.zeros( (len(pois), len(Weight)) , dtype = "float")
    
    cursor.execute("SELECT stationuid, lngmeter, latmeter from station")
    result = cursor.fetchall()
    hashmap = {}
    for row in result:
        hashmap[ row[0] ] = {"lngmeter" : float( row[1] ), "latmeter" : float( row[2] ) }

    print ("Constuct distanceMatrix")
    for i in range( len(pois) ):
        for j in range( len(Weight) ):
            stationuid = Weight[j]['stationuid']
            lngmeter = hashmap[ stationuid ]['lngmeter']
            latmeter = hashmap[ stationuid ]['latmeter'] 
            print ( "poilngmeter: %f, poilatmeter: %f " % ( pois[i]['lngmeter'], pois[i]['latmeter']))
            print ( "stationlngmeter: %f, station latmeter: %f " % ( lngmeter, latmeter ))
            print ( "distance: %f " % (distance( pois[i]['lngmeter'], pois[i]['latmeter'], lngmeter, latmeter) ) )
            distanceMatrix[i, j] = distance( pois[i]['lngmeter'], pois[i]['latmeter'], lngmeter, latmeter)
            weightdelay[i, j] = distanceDelay( distanceMatrix[i, j])

        print ("Constuct distanceMatrix...%f %%" % ( 100.0 * i / len(pois) ) )
    
    print ( distanceMatrix )
    print (weightdelay)

    print (" Construct done ")
    WeightVector = np.zeros( (len(Weight), 1), dtype = "float")

    for i in range(len(Weight)):
        WeightVector[i, 0] = float(Weight[i]['rank'])

    accessibilitys = weightdelay.dot(WeightVector)

    for i in range( len(pois) ):
        cursor.execute("UPDATE poi SET rank = (:accessibility) where uid = (:uid)", {"accessibility" : float(accessibilitys[i, 0]), "uid" : pois[i]['uid']})

    connect.commit()
    cursor.close()
    connect.close()
                    

    
    



if __name__ == "__main__" :
    #Crawler.InitDatabase()
    Location = "武汉市"
    for i in range(10):
        r = Crawler.nearBySearchCity(Location, pagesize = 100, pagenum = i)
        Crawler.insertPOI(r)
    

    
    with open("./buslines.txt", "r") as f:
        content = f.read()
        content = content
        items = content.split(" ")
        print (content)
        print (len(items))
        for item in items:
            
            uids = Crawler.getBusUid(item)
            print ("item name : %s-%d" % (item, len(uids)))
            print (uids)
            for i in uids:
                data = Crawler.getBusLine(i)
                #print ( data )
                Crawler.insertBusLine(data)
    accessibility()

    importPolygon()
    importPolyline()
    importPoint()