#!flask/bin/python
from flask import Flask, jsonify, abort
from influxdb import InfluxDBClient
import json

app = Flask(__name__)

tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol', 
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web', 
        'done': False
    }
]

app.config.update(dict(
  #  DATABASE=os.path.join(app.root_path, 'flaskr.db'),
    SERVER_NAME = "localhost:8080",
    DEBUG=True
  #  SECRET_KEY='development key',
 #   USERNAME='admin',
#    PASSWORD='default'
	))


def get_query_string(path):
    if path in topics:
	print "Exact topic match found!"
	return "select * from \"" + path + "\";"

    print "No match found"    
    return ""

@app.route('/solardata', methods = ['GET'])
def get_index():
    return jsonify( { 'tasks': tasks } )


@app.route('/solardata/<path:path_url>', methods = ['GET'])
def get_solardata(path_url):


    query = get_query_string(path_url)
    if query == "":
	abort(404)
	return
    result = client.query(query)

    return json.dumps(result)


if __name__ == '__main__':

    host = "localhost"
    port = 8086
    user = 'restapi'
    password = '1234' #Or something more sofisticated
    dbname = 'by-administrative-region'

    query = 'list series;'


    client = InfluxDBClient(host, port, user, password, dbname)    


    #print("Queying data: " + query)
    result = client.query(query)


    topics = []
    for item in result:
        topics.append(item["name"])
    
    

    app.run(debug = True)
    
    
