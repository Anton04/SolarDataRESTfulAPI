#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import pandas as pd
import InfluxDBInterface
import time
reload(InfluxDBInterface)
from ElasticsearchInterface import ESinterface
import IoTtoolkit
#reload(IoTtoolkit)
import sys
import mosquitto
import os
import argparse


def AreaProductionAlgorithm(SitesProductionBuf,sites,PowerStreams,EnergyStreams):
    dfPower = SitesProductionBuf.Data[PowerStreams]
    dfEnergy = SitesProductionBuf.Data[EnergyStreams]
    AreaProduction = pd.DataFrame()
    
    
    AreaProduction["Power"] = dfPower.sum(1)
    AreaProduction["nSumedPower"] = dfPower.count(1)
    #AreaProduction["PmaxSumed"]
    
    Pmax = pd.DataFrame(list(sites.loc["Pmax"]),index=list(sites.loc["online_since"]/1000),columns = ["Pmax"])#dfPower #AreaProduction
    Pmax.sort_index(inplace=True)
    Pmax["Pmax"] *= 1000
    Pmax["nMax"] =1

    #Save the period.
    Start = AreaProduction.index[0]
    Stop = AreaProduction.index[-1]
    
    #Add Pmax
    AreaProduction = AreaProduction.join(Pmax.cumsum(),how="outer")
    
    #Calculate PmaxSum
    df = pd.DataFrame(sites.loc["Pmax"]).T
    df = df.reindex_axis(sorted(df.columns), axis=1)
    df2 = dfPower.reindex_axis(sorted(dfPower.columns), axis=1)
    
    AreaProduction["PmaxSum"] = pd.DataFrame(df2.notnull().values * df.values, index = df2.index, columns = df.columns).sum(1)*1000
    
    #Add Energy and nSummedEnergy 
    AreaProduction = AreaProduction.join(pd.DataFrame(dfEnergy.ffill().sum(1),columns = ["Energy"]),how="outer")
    AreaProduction["nSumedEnergy"] = dfEnergy.count(1)
    
    
    #Cut out the part we need.
    AreaProduction.fillna(method="ffill", inplace = True)
    AreaProduction = AreaProduction.loc[Start:Stop]
    
    return AreaProduction
    

# <codecell>

def CalculateAreaProduction(area,Recalculate=False):
    global AreaProduction2
    global debugName
    
    print "\tSearching for production sites: ",
    sites = es.GetHitsMatchingPropDict("solar-sites-index","meta-data",json.loads(area["query"]))
    
    if sites.shape[1] == 0:
        return 
    
    SitesProduction = IoTtoolkit.Feed()
    
    PowerStreams = SitesProduction.CombineStreamsFromMulipleSources("Power",ProductionDB,sites.columns,"Power",Compressed=False)
    EnergyStreams = SitesProduction.CombineStreamsFromMulipleSources("Energy",ProductionDB,sites.columns,"Energy",Compressed=True)
    SitesProductionBuf = SitesProduction.GetBuffer()
    SitesProductionBuf.Size = 5000
    
    if Recalculate:
        SitesProductionBuf.Seek(0)
    else:
        #Recalculate last week and forward. 
        SitesProductionBuf.Seek(time.time() - (14 * 24 * 3600))
    
    print "\tprocessing             \r",
    
    while not SitesProductionBuf.EOF:
        AreaProduction = AreaProductionAlgorithm(SitesProductionBuf,sites,PowerStreams,EnergyStreams)
        try:
            AreaDB.Replace(area.name,AreaProduction,Compressed = False)
        except ValueError:
            print AreaProduction
            AreaProduction2 = AreaProduction
            debugName = area.name
            break
        SitesProductionBuf.Next()

    print "\tDone!             \r",    
# <codecell>

if __name__ == "__main__":
    
    #Parse arguments
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-h', dest='host', default="localhost", help='MQTT host send results to')
    parser.add_argument('-t', dest='topic', default="", help='MQTT topic to process')
    parser.add_argument('-m', dest='message', default="", help='MQTT message to process')

    args = parser.parse_args()

    #Get location of script
    path = os.path.abspath(os.path.dirname(sys.argv[0]))


    #Set up MQTT
    ip = "localhost"
    port = 1883
    user = "driver"
    password = "1234"
    prefix = "SolarAreaProductionProducer"
    
    mqtt=mosquitto.Mosquitto("AreaProductionProducer")
    mqtt.prefix = prefix
    mqtt.ip = ip
    mqtt.port = port
    #mqtt.clientId = clientId
    mqtt.user = user
    mqtt.password = password
                
    if mqtt != None:
        mqtt.username_pw_set(user,password)
    
    #mqtt.will_set( topic =  "system/" + prefix, payload="Idle", qos=1, retain=True)
    mqtt.connect(ip,keepalive=10)
    mqtt.publish(topic = "system/"+ prefix, payload="Updating", qos=1, retain=True)
    
    #Setup enviroment
    DataLink = InfluxDBInterface.InfluxDBInterface(path + "/" + "influxInterfaceCredentials2.json")

    LogDB = DataLink.databases[u'SolarLogdata']
    ProductionDB = DataLink.databases[u'SolarProductionSites']
    AreaDB = DataLink.databases[u'SolarProductionAreas']
    Test = DataLink.databases[u'test']

    es = ESinterface()


    print "Searching area definions... ",
    areas = es.GetHitsAsDataFrame("solar-area-index","meta-data")
    
    #areas = areas.iloc[:,-7:]

    now = time.time()
    
    for id in areas.columns: 
        print u"Area: %s" % (areas[id]["Name"]).encode('utf-8')
        #print "processing             \r",
        #time.sleep(0.5)
        CalculateAreaProduction(areas[id])
    
    print "All done!"
    
    mqtt.connect(ip,keepalive=10)
    #mqtt.publish(topic = "solardata/production/at", payload=str((TrailTime,LeadTime)), qos=1, retain=True) 
    mqtt.publish(topic = "solardata/area-production/lastrun", payload=now, qos=1, retain=True)    
    mqtt.publish(topic = "system/"+ prefix, payload="Idle", qos=1, retain=True)
    
