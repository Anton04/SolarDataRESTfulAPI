#!/bin/python

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
