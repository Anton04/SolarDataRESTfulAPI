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

  def AddStream(self,Name = None,Database = None, Series = None,Property = None,Timeout = None,TOMarker = None, Type = None,Compressed = True):
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
    SourceToNamesDict = {}
 
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
        SourceToNamesDict[Key] = []

      SourceDict[Key].append(Property)
      SourceToNamesDict[Key].append(Name)

      #Names
      Key2 = (Database,Series,Property)

      if not NameDict.has_key(Key2):
        NameDict[Key2] = []
      
      NameDict[Key2].append(Name)
      

    #Save
    self.SourceDict = SourceDict
    self.NameDict = NameDict
    self.SourceToNamesDict = SourceToNamesDict

    return (SourceDict,NameDict)


  def LoadBuffer(self,Start=None,Length=10,Reverse = False):

    Sources = self.SourceDict

    #Make a complete copy of names. 
    Names = self.NameDict.copy() 

    for Key in self.NameDict:
      Names[Key] = self.NameDict[Key][:]


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

    if MainFrame.shape[0] > Length:
      MainFrame = MainFrame.iloc[:Length]

    return MainFrame

  def SaveBuffer(self,Start=None,Length=10,Reverse = False):
    #Warn if duplicates.
    for Key in self.NameDict:
      if len(self.NameDict[Key]) > 1:
        print "Warning columns with the same source exists. Saving all of them will give unpredictable results."
        break;


    #Split up into induvidial dataframes in accorance with source dictionary.
    for Key in self.SourceToNamesDict:
      Names = SourceToNamesDict[Key]
      Database = Key[0]
      Serie = Key[1]

      #Get the ones belonging to this source. 
      df = self.Buffer[Names]

      #Compress those marked as compressed
      dfcomp = self.Compress(df)

      #Write over old data in the database
      Database.Replace(Serie,dfcomp,'s',False) 
      
    return 

  def Compress(self,df):

    #Make list of columns that will should be compressed.
    RowsToBeCompressed = self.DataStreams.columns[list(self.DataStreams.loc["Compressed"].values)]

    #Compress all those columns. 
    CompDf = (df[RowsToBeCompressed].diff()!= 0).replace(False,float("NaN")) * df

    #Merge them back to the original dataframe without touching the other columns. 
    df[CompDf.columns] = CompDf

    return df



  def GetPointsPreceeding(self,TimeStamp=None):

    Values = pd.DataFrame(index = ["Timestamp","Value"])

    #Loop through all. 
    for (Name,Properties) in self.DataStreams.iteritems():
      Database = Properties["Database"]
      Serie = Properties["Serie"]
      Property = Properties["Property"]


      (StreamTime,StreamValue) = Database.GetPrecedingValue(TimeStamp,Serie,Property)

      #if StreamTime == None:
      #  (StreamTime,StreamValue) = Database.GetSuccedingValue(Serie,Property,TimeStamp)

      Values[Name] = [StreamTime,StreamValue]

    return Values

  def GetFeedStart(self): 

    FirstTimestamp = 9999999999999

    #Loop through all and look for the stream that starts first. 
    for (Name,Properties) in self.DataStreams.iteritems():
      Database = Properties["Database"]
      Serie = Properties["Serie"]
      Property = Properties["Property"]

      Timestamp = Database.GetFirstTimestamp(Serie,Property)

      if Timestamp < FirstTimestamp:
        FirstTimestamp = Timestamp
      
    return Timestamp /1000.0


  def SetPointer(self,Timestamp = 0):

    if Timestamp == 0:
      Timestamp = self.GetFeedStart()

    df = self.GetPointsPreceeding(Timestamp)

    #Align columns with the stream decriptor. 
    df = df.reindex_axis(self.DataStreams.columns, axis=1)

    StartsAt = df.loc["Timestamp"].min()

    #Store
    self.Pointer = Timestamp
    self.PointerValues = df

    return self.Pointer

  #Returns a buffer from where the pointer was set and updates the pointer. 
  def GetBuffer(self,Length=10):
    #Load raw buffer
    df = self.LoadBuffer(self.Pointer,Length)

    #Align columns with the stream decriptor
    df = df.reindex_axis(self.DataStreams.columns, axis=1)

    #Update first row 
    df.iloc[0] = self.PointerValues.loc["Value"].values

    #Calculate new pointer data. 
    Pointer = df.index[-1]

    #Check if we are at the end of the feed. 
    if self.Pointer == Pointer:
      return None

    #Calculate new pointer values fill in missing starts and decompress.
    Values = pd.DataFrame(index = ["Timestamp","Value"])

    for (Name,Column) in df.iteritems():
      Stream = Column.dropna()
      StreamTime = Stream.index[-1]
      StreamValue = Stream.values[-1]

      Values[Name] = [StreamTime,StreamValue]

      

      if self.DataStreams[Name]["Compressed"] == True:
        df.ffill(inplace=True)
    


    #Store
    self.Pointer = Pointer
    self.PointerValues = Values

    self.Buffer = df

    return df

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
  
  
  
  
  
  
  
