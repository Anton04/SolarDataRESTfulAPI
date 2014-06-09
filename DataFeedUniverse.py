#!/bin/python
from pubsub import pub
import json
import mosquitto 

#Abstraction layer for accessing long time storage
class LTSInterface():
  def __init__(self):
    return
    
#Abstraction layer for accessing realtime system    
class RTSInterface():
  def __init__(self):
    return

#Abstraction layer for accessing a feed repository    
class REPInterface():
  def __init__(self):
    return

#Class implementing access to influxDB    
class InfluxFeedLTSInterface(InfluxDBClient):
  def __init__(self,config_param="influx.json"):
    
    if type(Defenition) == type(""):
      #Load database credentials from file
      fp = open(config_param,"r")
      self.config = json.load(fp)
      fp.close()
    elif type(Defenition) == type({}):
      self.config = config_param
    
    #Connect
    InfluxDBClient.__init__(self,self.config["host"], self.config["port"], self.config["user"], self.config["password"], self.config["database"])

  def GetLastTimeStamp(self,FluxId):

    result = self.query('select time from \"%s\" order desc limit 1;' % FluxId, time_precision='m')

    try:
      return float(result[0]["points"][0][0])/1000.0
    except:
      return 0.0

  def SendToInfluxDB(self,df,FeedId):
    #Series name
    #series = FeedId + "/raw_data" 
    
    rows = 0

    #Save each row
    for i in range(0,Data.shape[0]):
      timestamp = Data.irow(i)[0]
      column = ["time"]
      data = [int(timestamp*1000)]
      
      
      #Iterate each value and remove NANs
      for j in range(1,Data.shape[1]):
        if numpy.isnan(Data.iloc[i,j]):
          continue
          
        #Add key
        column.append(Data.keys()[j])
        data.append(Data.iloc[i,j])

      #If there where only nan on this row continue to next row. 
      if len(column) == 1:
        continue
          
      fdata = [{
          "points": [data],
          "name": FeedId,
          "columns": column
          }]

      self.write_points_with_precision(fdata,"m")
      
      rows += 1
        
    return rows  
    
#Class implementing access to MQTT via mosquitto    
class Mosquitto_RTSLink(RTSLink):
  def __init__(self):
    return
  
  
UniverseDef = {"Name":"Soldata",
               "ID":"12312-234234-2342",
               "LTS TYPE":"InfluxDB"
               "LTS DATA":{
                  "host":"livinglab2.powerprojects.se",
                   "port":8086,
                   "user":"uploader",
                   "password":"ryKkSSnveKVpUMROt8kqvZCGJXJveu8MkJO",
                   "database":"by-id"}
                "RTS TYPE":"MQTT"
                "RTS DATA":{}
              }

#Class implementing a feed universe defined by feed definitions a realtime data access and longterm strorage.
class Universe:
  def __init__(self,Defenition="DataFeedUniverse.json"):
    if type(Defenition) == type(""):
      File = open(Defenition,"r")
      self.DefDict = json.load(File)
      File.close()
    elif type(Defenition) == type({}):
      self.DefDict = Defenition
      
    if self.DefDict["LTS TYPE"] == "InfluxDB"
      self.LTS = InfluxFeedLTSInterface(self.DefDict["LTS DATA"])
      
    return
  
  def GetFeedFromDefenition(self,Defenition):
    if type(Defenition) == type(""):
      File = open(Defenition,"r")
      self.DefDict = json.load(File)
      File.close()
    elif type(Defenition) == type({}):
      self.DefDict = Defenition
      
    
            
    
#Class implementing a feed     
class Feed:
  def __init__(self,Defenition,Universe = None):
    self.Universe = Universe
    Streams = []
    
    
class Stream:
  def __init__(self,Feed,Name,RTS_ID = None,LTS_ID = None,KeepAlive=0):
    self.Feed = Feed
    self.Name = Name
    self.RTS_ID = RTS_ID
    self.LTS_ID = LTS_ID
    self.KeepAlive = KeepAlive
    
    return
  
  
