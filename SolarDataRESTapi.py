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

def getSolarObjects(keys,Index,DB,Name,subset=["_meta","_production"]):


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
    lowercase = request.args.get("lowercase",False,type=bool)

    print "___"*10
    print tail, since, until, lowercase

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
            reply["_meta"]["UUID"] = siteUUID

            if lowercase:
                reply["_meta"] = MakeDictLowerCase(reply["_meta"])
                

        #Production.
        if  "_production" in subset:
            q = ("select * from %s where time < %s and time > %s limit %i" % (siteUUID,until,since,tail))
            print q
            data = DB.query(q,'m')
            if len(data) > 0:
                reply["_production"] = data[0]
                reply["_production"].pop("name") 

                if lowercase:
                    reply["_production"]["columns"] = MakeListLowerCase(reply["_production"]["columns"]) 
            else:           
                reply["_production"] = {}

            reply["_production"]["UUID"] = siteUUID
            

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
        
        #Skip one level if its just one data source (not counting id). 
        if len(reply) == 2:
            replys.append(reply[reply.keys()[-1]])
        else:
            replys.append(reply)
        

    return {Name:replys, "_total_hits":res['hits']['total']}

def getSolarObject(uid,Index,DB,Name,subset=["_meta","_production"]):

    try:
        hit = es.get(index=Index, doc_type="meta-data", id=uid)
    except NotFoundError:
        abort(404)

#Get parameters in the request.
    tail = request.args.get("tail",1000,type=int)
    since = request.args.get("since","now()-7d")
    until = request.args.get("until","now()",type=int)
    lowercase = request.args.get("lowercase",False,type=bool)

    print "___"*10
    print tail, since, until, lowercase

    #Avoid doing to large requests
    if tail > 10000:
        abort(411)

    #Process the hits and requests additional data from influxDB

    siteUUID = hit["_id"]
    
    #Add ID.
    reply = {}  
    reply["_UUID"] = siteUUID

    #Meta data.
    if "_meta" in subset:
        
        reply["_meta"] = hit["_source"]
        reply["_meta"]["UUID"] = siteUUID

        if lowercase:
            reply["_meta"] = MakeDictLowerCase(reply["_meta"])
            

    #Production.
    if  "_production" in subset:
        q = ("select * from %s where time < %s and time > %s limit %i" % (siteUUID,until,since,tail))
        print q
        data = DB.query(q,'m')
        if len(data) > 0:
            reply["_production"] = data[0]
            reply["_production"].pop("name") 

            if lowercase:
                reply["_production"]["columns"] = MakeListLowerCase(reply["_production"]["columns"]) 
        else:           
            reply["_production"] = {}

        reply["_production"]["UUID"] = siteUUID
        

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
    
    #Skip one level if its just one data source (not counting id). 
    if len(reply) == 2:
        replys = reply[reply.keys()[-1]]

    return {Name:reply, "_total_hits":res['hits']['total']}

def MakeDictLowerCase(dictionary):
    new_dict = {}

    for key in dictionary:
        value = dictionary[key]

        #Convert key
        if type(key) == str or type(value) == unicode:            
            new_key = key.lower()
        else:
            new_key = key

        #Convert value
        if type(value) == str:            
            new_value = value.lower()
        else:
            new_value = value

        new_dict[new_key] = new_value

    return new_dict

def MakeListLowerCase(l):
    NewList = []

    for value in l:
        if type(value) == str or type(value) == unicode:
            new_value = value.lower()
        else:
            new_value = value

        NewList.append(new_value)

    return NewList


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

####################
#   Get site by id  #
####################
@app.route('/solardata/site-by-id/<path:path_url>', methods = ['GET'])
def get_site_by_id_data(path_url):

    parts = get_parts(path_url)

    print parts

    if len(parts) > 2:
        abort(404)

    if parts[-1] == "_meta":
        return Response(json.dumps(getSolarObject(parts[:-1],"solar-sites-index",ProductionDB,"sites",[parts[-1]])), mimetype='application/json') #Respons(json.dumps(getMetadataSites(parts[:-1])),  mimetype='application/json')
    elif parts[-1] == "_production":
        return Response(json.dumps(getSolarObject(parts[:-1],"solar-sites-index",ProductionDB,"sites",[parts[-1]])), mimetype='application/json') #Response(json.dumps(getProductionDataSites(parts[:-1])), mimetype='application/json')
    elif parts[-1] == "_geography":
        return "Not implemented"

    return Response(json.dumps(getSolarObject(parts,"solar-sites-index",ProductionDB,"sites")), mimetype='application/json')
        

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
    
