#!/bin/python
from pubsub import pub
import json
import mosquitto 

#Abstraction layer for accessing long time storage
class LTSLink():
  def __init__(self):
    return
    
#Abstraction layer for accessing realtime system    
class RTSLink():
  def __init__(self):
    return

#Abstraction layer for accessing a feed repository    
class REPLink():
  def __init__(self):
    return

#Class implementing access to influxDB    
class InfluxDB_LTSLink(LTSLink):
  def __init__(self):
    return    
    
#Class implementing access to MQTT via mosquitto    
class Mosquitto_RTSLink(RTSLink):
  def __init__(self):
    return
  
  
UniverseDef = {"Name":"Soldata",
               "ID":"12312-234234-2342",
               "LTS TYPE":"InfluxDB"
               "LTS DATA":{}}

#Class implementing a feed universe defined by feed definitions a realtime data access and longterm strorage.
class Universe:
  def __init__(self,Defenition="DataFeedUniverse.json"):
    if type(Defenition) == type(""):
      File = open(Defenition,"r")
      self.DefDict = json.load(File)
    elif type(Defenition) == type({}):
      self.DefDict = Defenition
      
    if self.DefDict["LTS TYPE"] == "InfluxDB"
      self.LTS = 
      
    return
  
  
            
    
#Class implementing a feed     
class Feed:
  def __init__(self,Defenition):
    
