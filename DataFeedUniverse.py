#!/bin/python
from pubsub import pub
import json
import mosquitto 
from InfluxDBInterface import InfluxDBInterface

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


    
#Class implementing access to MQTT via mosquitto    
class Mosquitto_RTSLink(RTSLink):
  def __init__(self):
    return
  
  


#Class implementing a feed universe defined by feed definitions a realtime data access and longterm strorage.
class Universe:
  def __init__(self,Defenition="Universe.json",AutoloadFeeds = True,Active = False):
    
    self.Active = Active
    
    if Defenition == None:
      #Produce new Universe
      #Not implemented
      return
      
    elif type(Defenition) == type(""):
      File = open(Defenition,"r")
      self.DefDict = json.load(File)
      File.close()
      self.ID = DefDict["ID"]
      
      if AutoloadFeeds:
        self.LoadFeedsFromFile(DefDict["Feeds"])
      
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
      
  def LoadFeedsFromFile(self,FileName):
    File = open(FileName,"r")
    self.FeedsList = json.load(File)
    File.close()
    self.LoadFeedsFromList(FeedList)
    
  def LoadFeedsFromList(self,FeedsList)
    for Feed in FeedList:
      print "Feed"
      
    return
  
  
  def SaveFeedsToFile(self,FileName):
    return
            
    
#Class implementing a feed     
class Feed:
  def __init__(self,Defenition,Universe = None):
    self.Universe = Universe
    Streams = []
    
#Class implementing a stream.    
class Stream:
  def __init__(self,Defenition,Feed):
    self.Feed = Feed
    self.Name = Name
    self.RTS_ID = RTS_ID
    self.LTS_ID = LTS_ID
    self.KeepAlive = KeepAlive
    self.DerivedFromStreams = None
    self.SampleRate = None
    self.Threshhold = None
    
    return
  
  def GetStreamDefenition(self):
    Defenition = {}
    Defenition["Name"] = self.Name
    Defenition["RTS ID"] = self.RTS_ID
    Defenition["LTS ID"] = self.LTS_ID
    
    return Defenition
    
    
  
if __name__ == "__main__":
  MyUniverse = Universe(UniverseDef,AutoloadFeeds = False)
  
  MyUniverse.LoadFeedsFromFile("SLBFeeds.json")
  
  MyUniverse.GetFeedByName("").UpdateLTS()
  
  
  
  
  
  
  
