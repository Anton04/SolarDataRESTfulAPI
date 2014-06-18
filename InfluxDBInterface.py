#!/bin/python
from influxdb import InfluxDBClient
import json

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
    print self.config

    self.databases = {}

    for db in self.config:
    	database = InfluxDBClient(db["host"], db["port"], db["user"], db["password"], db["database"])
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

  def GetLastTimeStamp3(self,database,series,property):

    result = self.databases[database].query('select %s from \"%s\" order desc limit 1;' % (property,series), time_precision='m')

    print result 

    try:
      return float(result[0]["points"][0][0])/1000.0
    except:
      return 0.0

  def GetLastValue3(self,database,series,property):

    result = self.databases[database].query('select %s from \"%s\" order desc limit 1;' % (property,series), time_precision='m')

    print result

    try:
      return float(result[0]["points"][0][0])/1000.0
    except:
      return 0.0


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
