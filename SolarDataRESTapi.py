#!/bin/python
from flask import Flask, jsonify, abort,request,Response
import InfluxDBInterface
import json
from elasticsearch import Elasticsearch
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

def getmetadata(parts):

    if parts[-2] == "sites":
	return getMetadataSites(parts)
    elif parts[-2] == "parts":
	return "Not implemented"
    elif parts[-2] == "administrative_areas":
        return "Not implemented"    

    abort(404)
            
    return

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

    result = []

    for filename in os.listdir(path):
        if filename.endswith(".geojson"):
            file = open(path + "/" + filename,"r")
            data = json.load(file)
            file.close()

            result.append(data)
       	   
    return result

#Return the metadata limided by the keys sent. 
def getMetadataSites(keys):

    print keys

    index = ["Country","County","Municipality","Administrative_area","Citypart"]

    query = []

    l = len(keys)

    if l == 0:
	totalquery = {"size":1000,"query": {"match_all": {} }}

    else:

        for f in range(0,l):
            query.append({"match" : {index[f]:keys[f]} })


        totalquery = {"size":1000,"query": {"bool": {"must":query}  }}
 

    res = es.search(index="solar-sites-index",doc_type="meta-data", body=totalquery)


    print("Got %d Hits:" % res['hits']['total'])

    #print res
    #print "  "

    #Make a nicer list
    reply = []
    for hit in res['hits']['hits']:
        reply.append({hit["_id"]:{"_meta":hit["_source"]}})
        #print("Added: %(Owner)s: %(Address)s" % hit["_source"])
    
    return {"sites":reply, "_total_hits":res['hits']['total']}

def getProductionDataSites(keys):
    print keys

    index = ["Country","County","Municipality","Administrative_area","Citypart"]

    query = []

    l = len(keys)

    if l == 0:
        totalquery = {"size":1000,"query": {"match_all": {} }}

    else:

        for f in range(0,l):
            query.append({"match" : {index[f]:keys[f]} })


        totalquery = {"size":1000,"query": {"bool": {"must":query}  }}
 

    res = es.search(index="solar-sites-index",doc_type="meta-data", body=totalquery)


    print("Got %d Hits:" % res['hits']['total'])

    #print res
    #print "  "

    #Get parameters 
    tail = request.args.get("tail",1000,type=int)
    since = request.args.get("since","now()-7d")
    until = request.args.get("until","now()",type=int)

    print "___"*10
    print tail, since, until


    if tail > 10000:
	abort(411)

   

    #Make a nicer list
    reply = []
    for hit in res['hits']['hits']:
        siteUUID = hit["_id"]
        q = ("select * from %s where time < %s and time > %s limit %i" % (siteUUID,until,since,tail))
        print q
        data = ProductionDB.query(q,'m')        
        #data[0]['columns'][2]="Power"
        #data[0]['columns'][3]="Energy_counter"

        #data = []
        reply.append({siteUUID:data})
        #print("Added: %(Owner)s: %(Address)s" % hit["_source"])

    return {"sites":reply, "_total_hits":res['hits']['total']}


def get_query_string(path):
    #Check for exact match
    if path in topics:
	print "Exact topic match found!"
	return "select * from \"" + path + "\";"

    #Check for property match 
    parts = path.split("/")
    lastpart = parts[-1]
    firstpart = path[:(-len(lastpart)-1)]

    print "DEBUG"
    print lastpart
    print firstpart

    if firstpart in topics:
        print "Partial match"
        return "select "+ lastpart +" from \"" + firstpart + "\";"

    print "No match found"    
    return ""

def get_parts(path_url):
    #Remove trailing slash
    if path_url[-1] == "/":
        path_url = path_url[:-1]
    
    parts = path_url.split("/")

    return parts

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
        return Respons(json.dumps(getMetadataSites(parts[:-1])),  mimetype='application/json')
    elif parts[-1] == "_production":
        return Response(json.dumps(getProductionDataSites(parts[:-1])), mimetype='application/json')
    elif parts[-1] == "_geography":
        return "Not implemented"
        
    return "Not implemented"

####################
#   Get area data  #
####################
@app.route('/solardata/areas/<path:path_url>', methods = ['GET'])
def get_area_data(path_url):

    parts = get_parts(path_url)

    print parts

    if parts[-1] == "_meta":
        return Response(json.dumps(getMetadataAreas(parts[:-1])),  mimetype='application/json')
    elif parts[-1] == "_geography":
        return Response(json.dumps(getGeographyData(parts[:-1])),  mimetype='application/json')
    elif parts[-1] == "_production":
        return "Not implemented"

    return "Not implemented"


@app.route('/solardata/by-administrative-region/<path:path_url>', methods = ['GET'])
def get_solardata(path_url):

    #request.args.get

    parts = get_parts(path_url)

    print parts

    if parts[-1] == "meta":
	return json.dumps(getmetadata(parts))	
    elif parts[-1] == "production":
        return "Not implemented"
    elif parts[-1] == "geography":
        return "Not implemented"

    print path_url

    	    

    #result = getmedadatasites()    

    #query = get_query_string(path_url)
    #if query == "":
    abort(404)
    #	return
    #result = client.query(query)

    return 


if __name__ == '__main__':

    #import InfluxDBInterface

    #datalink = InfluxDBInterface.InfluxDBInterface("influxInterfaceCredentials.json")


    #InfluxDB interface. 
    DataLink = InfluxDBInterface.InfluxDBInterface("influxInterfaceCredentials2.json")
    LogDB = DataLink.databases[u'SolarLogdata']
    ProductionDB = DataLink.databases[u'SolarProductionSites']


    topics = DataLink.listdataseries()


    es = Elasticsearch()    
    

    #app.run(host = "0.0.0.0")
    app.run(debug = True)
    
