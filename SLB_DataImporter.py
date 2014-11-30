#!/usr/bin/python
# -*- coding: utf-8 -*-


#import requests
import pandas
import time
import json
#from influxdb import InfluxDBClient
#import numpy
import mosquitto 
from IPython.display import clear_output
import sys
from ElasticsearchInterface import ESinterface
import os
import traceback
import InfluxDBInterface

#Change to elasticsearch     
def LoadSiteIds(file="SiteIDs.json"):
    fp = open(file,"r")
    dic = json.load(fp)
    fp.close()
    return dic

def LoadSLBSiteIds(elasticsearch):
    #Elastic search search SLB operator
    sites = elasticsearch.GetHitsMatchingPropDict("solar-sites-index","meta-data",{"data_collection_operator":"SLB"})
    
    #for each hit read operator id and system id into dict. 
    ret = {}

    for site in sites:
        #print type(site)
        OperatorID = sites[site]["Operator_ID"] +"t"
        OurID = site
        ret[OperatorID] = OurID 
    
    #Return dict 
    return ret
    
def ParseSLBData(slb_id="h00t",start=time.time()-(24*60*60),stop=time.time()):
  
  starttime = time.strftime("%y%m%d%H%M",time.localtime(start))
  stoptime = time.strftime("%y%m%d%H%M",time.localtime(stop))
  url = "http://slb.nu/soldata/index.php?KEY=%s&start=%s&stop=%s" %(slb_id,starttime,stoptime)

  df = pandas.read_csv(url,sep = ";",parse_dates=[[0, 1]],skiprows=9, header = None ,infer_datetime_format = True,na_values = ["     ","    ","  "," ",""])
  cl = pandas.read_csv(url,sep = ";", header = 7,error_bad_lines= False,na_values = [""],nrows=1)

  #Align keys to data and rename time col. 
  cols = cl.keys()
  cols = cols[2:]
  col2 = cols.insert(0,"Time")
  col2 = col2.insert(-1,"NAN")

    
  #Remove SLB station id from key.
  NewCols = []
    
  lkey = len(slb_id)

  for datakey in col2:
        
    newkey = datakey
    
    #Remove SLB id from key
    if newkey.find(slb_id) != -1:
        newkey = newkey[lkey+1:]
    
        #Remove leading 0
        if newkey[0] == "0":
            newkey = newkey[1:]

    
    NewCols.append(newkey)
      
  #Set data keys as column descriptors
  df.columns = NewCols
  
  #Delete trailing columns with junk. 
  #for key in df.keys()[-5:df.shape[1]-1]:
  #    if key.find(slb_id) == -1:
  #        del df[key]

  #Reformat timestamps
  droplist = []

  for i in range(0,df.shape[0]):
    try:
      #print "*" + df["Time"][i]
      timestamp = time.mktime(time.strptime(df["Time"][i],"%y-%m-%d %H:%M"))
      df["Time"][i] = timestamp
    except:
      #print "*" + df["Time"][i]
      droplist.append(df.index[i])

  df = df.drop(droplist)

  return df
  




def Update(DB):

    #Set up MQTT
    ip = "localhost"
    port = 1883
    user = "driver"
    password = "1234"
    prefix = "SLBimporter"
    
    mqtt=mosquitto.Mosquitto("SLB importer")
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
    
    print "Starting update..."
    
    time.sleep(0.5)

    #Load sites.
    
    #ParseCVS("testdata/h00t_1310160720_1406030410.csv")
    SiteIDs = LoadSLBSiteIds(es)
    #SiteIDs = LoadSiteIds("/root/git/SolarDataRESTfulAPI/SiteIDs.json")
    
    #Get all data until now + 1h
    StopTime = time.time() + 3600
    
    sum_rows = 0
    
    for Site in SiteIDs:
        FeedId = "%s" % SiteIDs[Site]
        StartTime = DB.GetLastTimeStamp(FeedId)

        if StartTime == None:
            StartTime = 0
        
        if StartTime == 0:
            print "No previous records in: " + FeedId
            print "\tStarting from Okt 2013"
            StartTime = time.mktime(time.strptime("2013-10-01","%Y-%m-%d"))
            exit()
        else:
            print "Last record in stream: " + FeedId
            print "\tat: " + time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(StartTime))
            
        #time.sleep(0.5)
        sys.stdout.flush()
        
        #Start a tiny bit after the last value.
        Current = StartTime + 0.5        
        PeriodLen = 60*60*24*7
        
        
            
        
        LeadTime = 0
        TrailTime = 99999999999999999
        
        while Current < StopTime:
            
            #Dont request (to much) data in the future.
            if Current > StopTime:
                PeriodLen = StopTime - Current
                
            #But keep period to over 10 min atleast 
            if PeriodLen < 600:
                PeriodLen = 600
            
            print "\tReading SLB data from: " + Site 
            print "\tFrom: " + time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(Current))
            print "\tTo:   " + time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(Current + PeriodLen))
            Data = ParseSLBData(Site,Current,Current + PeriodLen)
            
            #Remove duplicate
            if Data["Time"][0] == StartTime:
              Data = Data.drop(Data.index[0])
                
            
            
            Current += PeriodLen
            print "Sending data to influx as: " + FeedId
            
            r = DB.SendToInfluxDB(Data,FeedId)
            print "%i Rows written" % r
            
            sum_rows += r
            
            AtTime = DB.GetLastTimeStamp(FeedId)
            
            if r > 0:
                mqtt.connect(ip,keepalive=10)
                mqtt.publish(topic = "solardata/sites/"+ FeedId + "/at", payload=AtTime, qos=1, retain=True)
            
            if AtTime > LeadTime:
                LeadTime = StopTime
            
            if AtTime < TrailTime:
                TrailTime = StopTime
        
    mqtt.connect(ip,keepalive=10)

    #Update operator data if anything was recived. 
    if sum_rows > 0:
        mqtt.publish(topic = "solardata/Operator/SLB/at", payload=str((TrailTime,LeadTime)), qos=1, retain=True) 
        mqtt.publish(topic = "solardata/Operator/SLB/lastupdate", payload=StopTime, qos=1, retain=True)    
        
    mqtt.publish(topic = "solardata/Operator/SLB/lastrun", payload=StopTime, qos=1, retain=True)
    mqtt.publish(topic = "system/"+ prefix, payload="Idle", qos=1, retain=True)
    time.sleep(0.5)
    
    del mqtt
    
    print "Finnished update!"
    sys.stdout.flush()
    
    return (TrailTime,LeadTime)


# <codecell>

if __name__ == "__main__":

    #Get location of script
    path = os.path.abspath(os.path.dirname(sys.argv[0]))
    
    es = ESinterface()

    #Init resources
    DataLink = InfluxDBInterface.InfluxDBInterface(path + "/" + "influxInterfaceCredentials2.json")
    LogDB = DataLink.databases[u'SolarLogdata']

    while True:
        Now = time.time()
        
        try:
            (TrailTime,LeadTime) = Update()
        except Exception,e: 
            #print str(e)
            print traceback.format_exc()
            print "Sleeping 1 min."
            sys.stdout.flush()
            time.sleep(60)
            print "Resuming"
            sys.stdout.flush()
            continue
            
        #Next data from SLB is expected in 10 minutes. No need to do anything before that. 
        NextData = TrailTime + 600
        TimeToNext = NextData - NextData
        
        if TimeToNext > 0:
            print "Sleeping %i seconds until next data is due to arrive" % int(TimeToNext)
            sys.stdout.flush()
            time.sleep(TimeToNext)
        else:
            print "Sleeping 2 min to see if new data has arrived."
            sys.stdout.flush()
            time.sleep(120)
            
        clear_output()
        
        

# <codecell>

print "test"

# <codecell>


