#!/bin/python
from influxdb import InfluxDBClient
import json
import pandas as pd

class InfluxDBlayer(InfluxDBClient):  

  def GetLastTimestamp(self,series,property = "*",time_precision='m'):
    return self.GetLastValue(series,property,time_precision)[0]

  def GetFirstTimestamp(self,series,property = "*",time_precision='m'):
    return self.GetFirstValue(series,property,time_precision)[0]

  def ListSeries(self):
    res = self.query("list series")

    ret = []

    for series in res:
       ret.append(series["name"])
    return ret

  def GetProperties(self,series):
    res = self.query("select * from %s limit 1" % series )
    if res == []:
       return []
    return res[0]["columns"][2:]

  def GetDataAfterTime(self,series,properties="*",timestamp=None,limit=10,time_precision='s'):

    #Handle indexing instead of name
    if type(series) == int():
	Series = self.ListSeries()
        Series.sort()
        series = Series[series]
        print "Series %s selected" % series

    #If no time specified start from the beginning. 
    if timestamp == None:
        timestamp == self.GetFirstTimestamp(series,properties,'m')/1000.0

    if type(properties) == type([]):
        combined = ""
        for part in properties:
            combined += part + ", "

        properties = combined[:-2]

    qstring = "select %s from %s where time > %i order asc limit %i" % (properties,series,int(timestamp*1000000000),limit)
    res = self.query(qstring,time_precision)

    return self.ResultToDataframe(res)

  def ResultToDataframe(self,result):
    if result == []:
      return None

    df = pd.read_json(json.dumps(result[0]["points"]))
    df.columns = result[0]["columns"]
    df.index = df["time"]

    df = df.drop(["time","sequence_number"],1)
    df = df.reset_index().groupby(df.index.names).first()

    return df

  def GetDataPeriod(self,series,properties,start,lenght=60*60*24*7,limit=1000,time_precision='s'):

    start = int(start*1000000)
    lenght = int(lenght*1000000)
    stop = start + lenght

    if type(properties) == type([]):
	combined = ""
        for part in properties:
            combined += part + ", "

        properties = combined[:-2] 


    qstring = "select %s from %s where time > %iu and time < %iu limit %i" %(properties,series,start,stop,limit)

    res = self.query(qstring,time_precision)

    return self.ResultToDataframe(res)


	

  def GetPropertiesPartiallyMatchingAbutNotB(self,series,keyA,keyB):
    res = self.GetPropertiesPartiallyMatching(series,keyA)

    ret = []

    for property in res:
       if property.find(keyB) != -1:
           continue
       ret.append(property)

    return ret


  def GetPropertiesPartiallyMatching(self,series,key):

    properties = self.GetProperties(series)

    ret = []

    for property in properties:
        if property.find(key) == -1:
            continue

        ret.append(property)

    return ret

  def GetLastValue(self,series,property = "*",time_precision='m'):

    result = self.query('select %s from \"%s\" order desc limit 1;' % (property,series), time_precision)

    #print result

    try:
      ret = result[0]["points"][0][2:]
      time = result[0]["points"][0][0]
      if len(ret) == 1:
          return (time,ret[0])
      elif len(ret) == 0:
          return (None,None)
      else:
          return (time,ret)
    except:
      return (None,None)

  def GetFirstValue(self,series,property = "*",time_precision='m'):

    result = self.query('select %s from \"%s\" order asc limit 1;' % (property,series), time_precision)

    #print result

    try:
      ret = result[0]["points"][0][2:]
      time = result[0]["points"][0][0]
      if len(ret) == 1:
          return (time,ret[0])
      elif len(ret) == 0:
          return (None,None)
      else:
          return (time,ret)
    except:
      return (None,None)

  def Save(self,Series,DataFrame):
    #Series name
    #series = FeedId + "/raw_data" 

    rows = 0

    #Save each row
    for timestamp in DataFrame.index:
      column = ["time"]
      data = [int(timestamp*1000)]


      #Iterate each value and remove NANs
      for key in DataFrame.columns:
        value = DataFrame.loc[timestamp,key]
        #print value 
        #print timestamp,key
 
        if pd.isnull(value):
          continue

        #Add key
        column.append(key)
        data.append(value)

      #If there where only nan on this row continue to next row. 
      if len(column) == 1:
        continue

      fdata = [{
          "points": [data],
          "name": Series,
          "columns": column
          }]

      self.write_points_with_precision(fdata,"m")

      rows += 1

    return rows

#Class implementing access to influxDB    
class InfluxDBInterface():
  def __init__(self,config_param="influxInterfaceCredentials.json"):
    
    if type(config_param) == type(""):
      #Load database credentials from file
      fp = open(config_param,"r")
      self.config = json.load(fp)
      fp.close()
    elif type(config_param) == type({}):
      self.config = config_param
    
    #Connect
    #print self.config

    self.databases = {}

    for db in self.config:
    	database = InfluxDBlayer(db["host"], db["port"], db["user"], db["password"], db["database"])
	self.databases[db["database"]]=database

    return 

  def GetDatabaseFromTopicPath(self,topic):
    dbname = topic.split("/")[0]
    dbname = dbname.strip("/")

    if dbname in self.databases:
	return self.databases[dbname]
    
    return None

  def GetLastTimeStamp(self,topic):

    result = self.GetDatabaseFromTopicPath(topic).query('select time from \"%s\" order desc limit 1;' % topic, time_precision='m')

    try:
      return float(result[0]["points"][0][0])/1000.0
    except:
      return 0.0

  def GetLastTimeStamp2(self,database,series):

    result = self.databases[database].query('select time from \"%s\" order desc limit 1;' % series, time_precision='m')

    try:
      return float(result[0]["points"][0][0])/1000.0                
    except:
      return 0.0

  def GetLastValue3(self,database,series,property):
  
    result = self.databases[database].query('select %s from \"%s\" order desc limit 1;' % (property,series), time_precision='m')
    
    #print result
    
    try:
      ret = result[0]["points"][0][2:]
      if len(ret) == 1:
          return ret[0]
      elif len(ret) == 0:
          return None
      else:
          return ret
    except:
      return None

  def GetLastTimeStamp3(self,database,series,property):

    result = self.databases[database].query('select %s from \"%s\" order desc limit 1;' % (property,series), time_precision='m')

    #print result 

    try:
      return float(result[0]["points"][0][0])/1000.0
    except:
      return 0.0

  def GetLastValue3(self,database,series,property):

    result = self.databases[database].query('select %s from \"%s\" order desc limit 1;' % (property,series), time_precision='m')

    #print result

    try:
      ret = result[0]["points"][0][2:]
      if len(ret) == 1:
          return ret[0]
      elif len(ret) == 0:
          return None
      else: 
          return ret
    except:
      return None


  def listdataseries(self):
    series = []
    for dbname in self.databases:
        database = self.databases[dbname]
	result = database.query("list series;")

	for item in result:
            series.append(dbname + "/" +item["name"])

    return series

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
