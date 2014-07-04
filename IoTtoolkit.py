#!/bin/python
#from pubsub import pub
import json
import mosquitto 
from InfluxDBInterface import InfluxDBInterface
import pandas as pd
import uuid

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
class Mosquitto_RTSLink(RTSInterface):
  def __init__(self):
    return
  
  


#Class implementing a feed universe defined by feed definitions a realtime data access and longterm strorage.
class Universe:
  def __init__(self,Defenition="Universe.json",AutoloadFeeds = True,Active = False):
    
    self.Active = Active
    self.Feeds = []
    
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

    self.Name = self.DefDict["Name"]
    self.ID = self.DefDict["ID"]
      
    if self.DefDict["LTS TYPE"] == "InfluxDB":
      self.LTS = InfluxDBInterface(self.DefDict["LTS DATA"])

    if AutoloadFeeds == True:
      self.FeedFile = self.DefDict[FEEDS]
      self.LoadFeedsFromFile(self.FeedFile)
      
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
    FeedsList = json.load(File)
    File.close()
    self.LoadFeedsFromList(FeedList)
    
  def LoadFeedsFromList(self,FeedsDsc):

    FeedList = FeedsDsc["FEEDLIST"]

    #Check if Feeds are compatible with this universe.
    if FeedsDsc["Universe"] != self.ID:
      print "ERROR: Feed description file belong to other universe"
      return

    #Add all the feeds
    for FeedDsc in FeedList:
      self.Feeds.append(Feed(FeedDsc,self))
      
    return
  
  
  def SaveFeedsToFile(self,FileName):
    return
            
    
#Class implementing a feed     
class Feed():
  def __init__(self,Universe = None):

    self.Universe = Universe

    self.DataStreams = pd.DataFrame(index = ["Database","Serie","Property","Timeout","TOMarker","Type","Compressed"])
    self.Pointer = None
    self.Buffer = None

  def AddStream(self,Name = None,Database = None, Series = None,Property = None,Timeout = None,TOMarker = None, Type = None,Compressed = False):
    if Name == None:
      Name = uuid.uuid1()

    self.DataStreams[Name] = pd.Series([Database,Series,Property,Timeout,TOMarker,Type,Compressed],index=self.DataStreams.index)

    self.UpdateSourceAndNameDirectories()

    return self.DataStreams[Name]    

  def RemoveStream(self,Id = None):
    if Id == None:
      return False

    try:

      if type(Id) == str:
        Stream = self.DataStreams[Id]
        self.DataStreams = self.DataStreams.drop([Id],axis=1)
      elif type(Id) == int:
        Stream = self.DataStreams[self.DataStreams.columns[Id]]
        self.DataStreams = self.DataStreams.drop(self.DataStreams.columns[Id],axis=1)
    except KeyError:
      return False

    self.UpdateSourceAndNameDirectories()

    return Stream

  def UpdateSourceAndNameDirectories(self):

    SourceDict = {}
    NameDict = {}
 
    #Sort dbs and series.
    for Stream in self.DataStreams.iteritems():
      Database = Stream[1]["Database"]
      Series = Stream[1]["Serie"]
      Property = Stream[1]["Property"]
      Name = Stream[0]

      #Sources
      Key = (Database,Series)

      if not Key in SourceDict:
        SourceDict[Key] = []

      SourceDict[Key].append(Property)

      #Names
      Key2 = (Database,Series,Property)

      if not Key2 in NameDict:
        NameDict[Key2] = []

      NameDict[Key2].append(Name)

    #Save
    self.SourceDict = SourceDict
    self.NameDict = NameDict

    return (SourceDict,NameDict)


  def LoadBuffer(self,Start=None,Length=10,Reverse = False):

    Sources = self.SourceDict.copy()
    Names = self.NameDict.copy() 

    #Load each source into a frame. 
    Frames = []

    for Key in Sources:
      (Database,Series) = Key
      if Reverse:
        Res = Database.GetDataBeforeTime(Series,Sources[Key],Start,Length)
      else:
        Res = Database.GetDataAfterTime(Series,Sources[Key],Start,Length)

      if type(Res) != pd.DataFrame:
        continue

      #Replace properties with names
      NameList = []

      for Property in Res.columns:
        NameList.append(Names[((Database,Series,Property))].pop())

      Res.columns = NameList

      Frames.append(Res)

    #Join all frames
    MainFrame = Frames[0]

    for Frame in Frames[1:]:
        MainFrame = MainFrame.join(Frame, how='outer')

    return MainFrame.iloc[:Length]

   

  def GetPointsAround(self,TimeStamp=None):

    Values = pd.DataFrame(index = ["Timestamp","Value"])

    #Loop through all. 
    for (Name,Properties) in self.DataStreams.iteritems():
      Database = Properties["Database"]
      Serie = Properties["Serie"]
      Property = Properties["Property"]

      (StreamTime,StreamValue) = Database.GetPrecedingValue(Serie,Property,TimeStamp)

      if StreamTime == None:
        (StreamTime,StreamValue) = Database.GetSuccedingValue(Serie,Property,Time)

      Values[Name] = [StreamTime,StreamValue]

    return PointerValues

  def SetPointer(self,Timestamp = 0):

    df = self.GetPointsAround()

    StartsAt = df["Timestamp"].min()

    #If pointer is set ahead of feedstart adjust
    if StartsAt > TimeStamp:
      self.Pointer = StartsAt
    else:
      self.Pointer = TimeStamp

    #Store
    self.PointerValues = df

    return self.Pointer

          

     


#Class implementing a stream.    
class Stream:
  def __init__(self,Defenition,Feed):
    self.Feed = Feed
    self.ID = Defenition["ID"]
    self.Name = Defenition["Name"]
    self.RTS_ID = Defenition["RTS ID"]
    self.LTS_ID = Defenition["LTS ID"]

    if Defenition.has_key("KeepAlive"):
      self.KeepAlive = Defenition["KeepAlive"]

    if Defenition.has_key("Derrived"):
      self.Derrived = Defenition["Derrived"]
    else:
      self.Derrived = False

    if Defenition.has_key("Function"):
      self.Function = Defenition["Function"]
    else:
      self.Function = False

    #self.Expires = "1y"
    #self.SampleRate = Defenition["Name"] None
    #self.Threshhold = Defenition["Name"] None
    
    return
  
  def GetStreamDefenition(self):
    Defenition = {}
    Defenition["Name"] = self.Name
    Defenition["RTS ID"] = self.RTS_ID
    Defenition["LTS ID"] = self.LTS_ID
    
    return Defenition
    
    
  
if __name__ == "__main__":
  MyUniverse = Universe(UniverseDef,AutoloadFeeds = True)
  
  MyUniverse.LoadFeedsFromFile("SLBFeeds.json")
  
  MyUniverse.GetFeedByName("").UpdateLTS()
  
  
  
  
  
  
  
