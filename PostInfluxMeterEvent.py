#!/usr/bin/python
# -*- coding: utf-8 -*-

#Add path to API files. 
import sys
sys.path.append("/home/iot/repos/SolarDataRESTfulAPI")

import pandas as pd
import InfluxDBInterface
import time
from ElasticsearchInterface import ESinterface
import argparse
import json

def EpocToDate(timestamp):
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(timestamp))
    except:
        return "-"

def SecToHMS(sec):
    try:
        sec = int(sec)
        hour = sec / 3600
        minutes = (sec - (hour * 3600))/60
        secs = sec % 60
        return "%i h %i min %i s" %(hour,minutes,secs)
    except:
        return "-"

# <codecell>



    

# <codecell>

if __name__ == '__main__':

    parser = argparse.ArgumentParser(add_help=False)
    
    parser.add_argument('-h', dest='host', default="localhost", help='MQTT host send results to')
    parser.add_argument('-t', dest='topic', default="test/meterevent", help='MQTT topic to process')
    #parser.add_argument('-m', dest='message', default="", help='MQTT message to process')
    parser.add_argument('-u', dest='user', help='Username')
    parser.add_argument('-P', dest='password', help='Password')
    
    parser.add_argument('-c', dest='credentials_file', default="/home/iot/.credentials/influxInterfaceCredentials2.json", help='Credential file')    
    parser.add_argument('-d', dest='database', help='database')
    parser.add_argument('-s', dest='series', help='series')
    parser.add_argument('-p', dest='power_key', default="power", help='name of power key')
    parser.add_argument('-e', dest='energy_key', default="energy", help='name of energy key')

    args = parser.parse_args()

    DataLink = InfluxDBInterface.InfluxDBInterface(args.credentials_file)

    #LogDB = DataLink.databases[u'SolarLogdata']
    #ProductionDB = DataLink.databases[u'SolarProductionSites']
    #AreaDB = DataLink.databases[u'SolarProductionAreas']
    
    DataBase = DataLink.databases[args.database]

    #es = ESinterface()

    #UpdateStatusWebpage()
    (time_p,power) = DataBase.GetLastValue(args.series,[args.power_key])
    (time_e,energy) = DataBase.GetLastValue(args.series,[args.energy_key])
    
    if time_p != time_e:
        exit(1);
        
    meterevent = {"time":time_e/1000.0,"power":power,"energy":energy}
    
    payload = json.dumps(meterevent)

    #Connecing to the specified host 
    client = mosquitto.Mosquitto("InfluxMeterEvent")

    if args.user != None:
        client.username_pw_set(args.user,args.password)

    client.connect(args.host)
    print "Sending:"
    print args.topic
    print payload
    print "to" + args.host

    #Echoing the message recived. 
    client.publish(args.topic, payload, 1)
    client.disconnect()
    
