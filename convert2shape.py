import os
ARCPY = False
try:
    import arcpy
    ARCPY = True
except:
    print ("it seems that modual arcpy not exisits")

def createFeature(path, template, types, featureTypes = "POLYGON") :
    try:
        if os.path.exists(path):
            os.remove(path)
            os.remove(path[:-4] + ".dbf")
            os.remove(path[:-4] + ".shx")

        arcpy.CreateFeatureclass_management(os.path.dirname(path),
                                            os.path.basename(path),
                                            featureTypes)
        for i in range(len(template)):
            arcpy.AddField_management(path, template[i], types[i])

    except Exception as e:
        print ( e )

def constructPolygon( pointdata):
    points = []
    for (x, y) in pointdata:
        points.append( arcpy.Point(x, y) )

    spatial_reference = arcpy.SpatialReference(4326)
    array = arcpy.Array( points )
    return arcpy.Polygon(array, spatial_reference)

def constructPolyline( pointdata ):
    points = []
    for (x, y) in pointdata:
        points.append( arcpy.Point(x, y) )
    spatial_reference = arcpy.SpatialReference(4326)
    array = arcpy.Array( points )
    return arcpy.Polyline(array, spatial_reference)

def constructPoint(lng, lat):
    return (lng, lat)

def insertRow(shppath, fields, fieldsdata):
    cursor = arcpy.da.InsertCursor(shppath, fields)

    cursor.insertRow(fieldsdata)