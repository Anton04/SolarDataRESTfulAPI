#!/bin/python
from flask import Flask, jsonify, abort,request,Response
import InfluxDBInterface
import json
#from elasticsearch import Elasticsearch
from ElasticsearchInterface import ESinterface
import os

app = Flask(__name__)


app.config.update(dict(
  #  DATABASE=os.path.join(app.root_path, 'flaskr.db'),
#     SERVER_NAME = "livinglab2.powerprojects.se:8080"
     SERVER_NAME = "localhost:8088"
#    DEBUG=True
  #  SECRET_KEY='development key',
 #   USERNAME='admin',
#    PASSWORD='default'
	))


def getGeographyData(keys):

    path = "/var/www/html/geography/" + "/".join(keys)
    print path

    if os.path.isfile(path + ".geojson"):
        file = open(path + ".geojson","r") 
        data = json.load(file)
	file.close()
        return [data]

    if not os.path.exists(path):
        abort(404)

    features =[]
    crs = {}
    jsontype = {}

    for filename in os.listdir(path):
        if filename.endswith(".geojson"):
            file = open(path + "/" + filename,"r")
            data = json.load(file)
            file.close()

            features += data["features"]
            crs = data["crs"]
            jsontype = data["type"]
       	   
    return {"features":features,"crs":crs,"type":jsontype}

def get_parts(path_url):
    #Remove trailing slash
    if path_url[-1] == "/":
        path_url = path_url[:-1]
    
    parts = path_url.split("/")

    return parts

def getSolarObjects(keys,Index,DB,Name,subset=["_meta","_production"]]):


    #Map the keys in the request path to the following properties. 
    index = ["Country","County","Municipality","Administrative_area","Citypart"]

    #Produce the query for elastic search. 
    query = []

    #If now keys are supplied request all entries. 
    l = len(keys)
    if l == 0:
        totalquery = {"size":1000,"query": {"match_all": {} }}

    #Match each key
    else:
        for f in range(0,l):
            query.append({"match" : {index[f]:keys[f]} })


        totalquery = {"size":1000,"query": {"bool": {"must":query}  }}
 
    #Request data. 
    res = es.search(index=Index,doc_type="meta-data", body=totalquery)

    print("Got %d Hits:" % res['hits']['total'])

    #Get parameters in the request.
    tail = request.args.get("tail",1000,type=int)
    since = request.args.get("since","now()-7d")
    until = request.args.get("until","now()",type=int)

    print "___"*10
    print tail, since, until

    #Avoid doing to large requests
    if tail > 10000:
        abort(411)

    #Process the hits and requests additional data from influxDB
    replys = []
    for hit in res['hits']['hits']:
        siteUUID = hit["_id"]
        
        #Add ID.
        reply = {}  
        reply["_UUID"] = siteUUID

        #Meta data.
        if "_meta" in subset:
            reply["_meta"] = hit["_source"]

        #Production.
        if  "_production" in subset:
            q = ("select * from %s where time < %s and time > %s limit %i" % (siteUUID,until,since,tail))
            print q
            data = DB.query(q,'m')
            reply["_production"] = data[0]

        #Geography
        if "_geography" in subset:
            features = []
            #Request data. 
            #query.append({"match" : {index[f]:keys[f]} })
            #totalquery = {"size":1000,"query": {"bool": {"must":query}  }}
            #res = es.search(index=Index,doc_type="geography-data", body=totalquery)

            reply["_geography"] = {}
            reply["_geography"]["crs"] = {"type":"name","properties":{"name":"urn:ogc:def:crs:EPSG::3011"}}
            reply["_geography"]["type"] = "FeatureCollection"
            reply["_geography"]["features"] = features
        
        replys.append(reply)
        

    return {Name:replys, "_total_hits":res['hits']['total']}



@app.route('/solardata', methods = ['GET'])
def get_index():
    return jsonify( { 'tasks': tasks } )

####################
#   Get site data  #
####################
@app.route('/solardata/sites/<path:path_url>', methods = ['GET'])
def get_site_data(path_url):

    parts = get_parts(path_url)

    print parts

    if parts[-1] == "_meta":
        return Response(json.dumps(getSolarObjects(parts[:-1],"solar-sites-index",ProductionDB,"sites",[parts[-1]])), mimetype='application/json') #Respons(json.dumps(getMetadataSites(parts[:-1])),  mimetype='application/json')
    elif parts[-1] == "_production":
        return Response(json.dumps(getSolarObjects(parts[:-1],"solar-sites-index",ProductionDB,"sites",[parts[-1]])), mimetype='application/json') #Response(json.dumps(getProductionDataSites(parts[:-1])), mimetype='application/json')
    elif parts[-1] == "_geography":
        return "Not implemented"

    return Response(json.dumps(getSolarObjects(parts,"solar-sites-index",ProductionDB,"sites")), mimetype='application/json')
        

####################
#   Get area data  #
####################
@app.route('/solardata/areas/<path:path_url>', methods = ['GET'])
def get_area_data(path_url):

    parts = get_parts(path_url)

    print parts

    if parts[-1] == "_meta":
        return Response(json.dumps(getSolarObjects(parts[:-1],"solar-area-index",AreaDB,"areas",[parts[-1]])), mimetype='application/json')#Response(json.dumps(getMetadataAreas(parts[:-1])),  mimetype='application/json')
    elif parts[-1] == "_geography":
        return Response(json.dumps(getGeographyData(parts[:-1])),  mimetype='application/json')
    elif parts[-1] == "_production":
        return Response(json.dumps(getSolarObjects(parts[:-1],"solar-area-index",AreaDB,"areas",[parts[-1]])), mimetype='application/json')

    return Response(json.dumps(getSolarObjects(parts,"solar-area-index",AreaDB,"areas")), mimetype='application/json')



if __name__ == '__main__':

    #import InfluxDBInterface

    #datalink = InfluxDBInterface.InfluxDBInterface("influxInterfaceCredentials.json")


    #InfluxDB interface. 
    DataLink = InfluxDBInterface.InfluxDBInterface("influxInterfaceCredentials2.json")
    LogDB = DataLink.databases[u'SolarLogdata']
    ProductionDB = DataLink.databases[u'SolarProductionSites']
    AreaDB = DataLink.databases[u'SolarProductionAreas']

    #topics = DataLink.listdataseries()


    #es = Elasticsearch()    
    es = ESinterface()

    #app.run(host = "0.0.0.0")
    app.run(debug = True)
    
