#!/bin/python
from flask import Flask, jsonify, abort
from influxdb import InfluxDBClient
import json
from elasticsearch import Elasticsearch

app = Flask(__name__)


app.config.update(dict(
  #  DATABASE=os.path.join(app.root_path, 'flaskr.db'),
     #SERVER_NAME = "livinglab2.powerprojects.se:8080"
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

def getMetadataSites(keys):

    index = ["Country","County","Multiciparty","Administrative_area","Citypart"]

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

    reply = []
    for hit in res['hits']['hits']:
        site = hit["_source"]
        site["UUID"] = hit["_id"]
        reply.append(site)
        #print("Added: %(Owner)s: %(Address)s" % hit["_source"])
    
    return {"_meta":reply, "_total_hits":res['hits']['total']}

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


#Get site data
@app.route('/solardata/sites/<path:path_url>', methods = ['GET'])
def get_site_data(path_url):

    parts = get_parts(path_url)

    print parts

    if parts[-1] == "_meta":
        return json.dumps(getMetadataSites(path_url))
    elif parts[-1] == "_production":
        return "Not implemented"
    elif parts[-1] == "_geography":
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

    import InfluxDBInterface

    datalink = InfluxDBInterface.InfluxDBInterface("influxInterfaceCredentials.json")

    topics = datalink.listdataseries()


    es = Elasticsearch()    
    

    #app.run(host = "0.0.0.0")
    app.run(debug = True)
    
