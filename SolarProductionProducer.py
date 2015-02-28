#!/usr/bin/python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

#cd git/SolarDataRESTfulAPI/

# <codecell>

import json
import pandas as pd
import InfluxDBInterface
import time
reload(InfluxDBInterface)
from ElasticsearchInterface import ESinterface
import sys
import mosquitto
import os
import argparse


def EpocToDate(timestamp):
    return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(timestamp))
        
def SecToHMS(sec):
    sec = int(sec)
    hour = sec / 3600
    minutes = (sec - (hour * 3600))/60
    secs = sec % 60
    return "%i h %i min %i s" %(hour,minutes,secs)

def RemoveResets(series):
    
    FirstValue = series.iloc[0]
    change =  series.diff().clip(0)
    change.iloc[0] = FirstValue
    return change.cumsum()

def CalculateProduction(Site,LogDB,ProductionDB,Recalculate=False,mqtt=None):

    #Create property lists
    EnergyProp = LogDB.GetPropertiesPartiallyMatchingAbutNotB(Site,"POWc","Tot")
    PowerProp = LogDB.GetPropertiesPartiallyMatchingAbutNotB(Site,"Pac","Tot")
    
    PreviousLastValidValue = 0
    PreviousLastValidValueTime = 0
    
    #Determine where to resume.
    if Recalculate == False:
        (PreviousLastValidValueTime,PreviousLastValidValue) = ProductionDB.GetLastValue(Site,"Energy")
        TimestampP = ProductionDB.GetLastTimestamp(Site,"Power")

        if (PreviousLastValidValueTime != None and TimestampP != None):
        
            #The start from where we have both power and energy values. 
            if TimestampP < PreviousLastValidValueTime:
                PreviousLastValidValueTime = TimestampP
                
            PreviousLastValidValueTime = PreviousLastValidValueTime / 1000
                
            print "\tResuming calculation from: %s" % EpocToDate(PreviousLastValidValueTime)
                
            #Get last data. 
            dfLog = LogDB.GetDataAfterTime(Site,EnergyProp + PowerProp,PreviousLastValidValueTime,1000)
        else:    
            dfLog = LogDB.GetDataAfterTime(Site,EnergyProp + PowerProp,None,1000)
            print "No previous data starting from first log data."
    else:  
        #Get a log data chunck
        dfLog = LogDB.GetDataAfterTime(Site,EnergyProp + PowerProp,None,1000)
        
    
    while (dfLog.shape[0] > 1):


    
        #Create a production frame.
        dfProduction = pd.DataFrame(columns = ["Power","Energy"])
        
        
        #Calculate power
        dfProduction["Power"] = dfLog[PowerProp].sum(axis=1)
        
        #Calculate energy
        dfPOWc = dfLog[EnergyProp]
        dfProduction["Energy"] = dfPOWc.apply(RemoveResets).sum(axis=1)
        
        #Add offset from previus iteration.
        
        #Check if we have overlap. Is the last time the same as the smallest countervalue in the current array.
        FirstValidValueTime = dfProduction["Energy"].idxmin()
        
        #First time ever... or just NaN values in data. 
        if PreviousLastValidValueTime == None or pd.isnull(FirstValidValueTime):
            offset = 0
        #Normal overlap
        else:   
            offset = PreviousLastValidValue - dfProduction["Energy"][FirstValidValueTime]
        
        dfProduction["Energy"] += offset
        
        #Update database
        ProductionDB.Replace(Site,dfProduction)
        
        #Update mqtt
        if mqtt != None:
            ts = dfProduction.iloc[-1].name
            power = dfProduction.iloc[-1].Power
            energy = dfProduction.iloc[-1].Energy
            payload = json.dumps({"time":ts,"power":power,"energy":energy})
            mqtt.publish(topic = "solardata/sites/%s/meterevent" % Site, payload=payload, qos=1, retain=True) 
        
        #Keep track of counter max.
        MaxEnergyTime = dfProduction["Energy"].idxmax()
        
        if not pd.isnull(MaxEnergyTime):
            PreviousLastValidValue = dfProduction["Energy"][MaxEnergyTime]
            PreviousLastValidValueTime = MaxEnergyTime
        
        dfLog = LogDB.GetNextNRows(dfLog,1000)

        print "\tAt: %i        \r" % int(dfLog.index[-1]),
        sys.stdout.flush()
        
    return dfLog.index[-1]

# <codecell>

if __name__ == '__main__':

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
    prefix = "SolarProductionProducer"
    
    mqtt=mosquitto.Mosquitto("ProductionProducer")
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
    mqtt.loop_start()


    #Init resources 
    DataLink = InfluxDBInterface.InfluxDBInterface(path + "/" + "influxInterfaceCredentials2.json")
    LogDB = DataLink.databases[u'SolarLogdata']
    ProductionDB = DataLink.databases[u'SolarProductionSites']
    #es = ESinterface()

    #Init vars
    Sites = LogDB.ListSeries()
    now = time.time()
    
    #Loop throug all sites. 
    for Site in Sites:
     
        print "Processing %s " % Site 
        
        sys.stdout.flush()

        until = CalculateProduction(Site,LogDB,ProductionDB,False,mqtt)
        
        until = int(now - until)
        
        hour = until / 3600
        
        minutes = (until - (hour * 3600))/60
        
        secs = until % 60
        
        print "\tFinnished processing up to %i hours %i minutes and %i seconds from script start time" % (hour,minutes,secs)
        
        sys.stdout.flush()

    print "Done"

    sys.stdout.flush()

    mqtt.connect(ip,keepalive=10)
    #mqtt.publish(topic = "solardata/production/at", payload=str((TrailTime,LeadTime)), qos=1, retain=True) 
    mqtt.publish(topic = "solardata/production/lastupdate", payload=now, qos=1, retain=True)    
    mqtt.publish(topic = "system/"+ prefix, payload="Idle", qos=1, retain=True)
    time.sleep(0.5)
    
    del mqtt


