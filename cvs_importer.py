import requests
import pandas
import time
import json
from influxdb import InfluxDBClient
import numpy

def download_file(url):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    return local_filename
    
def LoadSiteIds(file="SiteIDs.json"):
    fp = open(file,"r")
    dic = json.load(fp)
    fp.close()
    return dic
    
def ParseSLBData(slb_id="h00t",start=time.time()-(24*60*60),stop=time.time()):
  
  starttime = time.strftime("%y%m%d%H%M",time.localtime(start))
  stoptime = time.strftime("%y%m%d%H%M",time.localtime(stop))
  url = "http://slb.nu/soldata/index.php?KEY=%s&start=%s&stop=%s" %(slb_id,starttime,stoptime)

  df = pandas.read_csv(url,sep = ";",parse_dates=[[0, 1]],skiprows=8, header = None ,infer_datetime_format = True,na_values = ["     ","    ","  "," ",""])
  cl = pandas.read_csv(url,sep = ";", header = 6,error_bad_lines= False,na_values = [""],nrows=1)

  #Align keys to data and rename time col. 
  cols = cl.keys()
  cols = cols[2:]
  col2 = cols.insert(0,"Time")
  col2 = col2.insert(-1,"NAN")

  #Remove SLB station id from key.
  NewCols = []

  for datakey in col2:
    NewCols.append(datakey.strip(slb_id))
  
  #Set data keys as column descriptors
  df.columns = NewCols
  
  #Delete trailing columns with junk. 
  #for key in df.keys()[-5:df.shape[1]-1]:
  #    if key.find(slb_id) == -1:
  #        del df[key]

  #Reformat timestamps
  for i in range(0,df.shape[0]):
    try:
      timestamp = time.mktime(time.strptime(df["Time"][i],"%y-%m-%d %H:%M"))
      df["Time"][i] = timestamp
    except:
      df = df.drop(df.index[i])
      
  return df
  

#{"TYPE":"Feed",
# "Name":"test",
# "ID":"23423-2423432-23432"
# "STREAMS":[{
#   "NAME":"MPOWc001",
#   "RTS TYPE":"MQTT",
#   "LTS TYPE":"INFLUXDB",
#   "RTS ID":"/SLB/h00t/MPOWc001",
#   "LTS ID":"23324-23432-2342/raw_data/MPOWc001",
#   "TIMEOUT":"600000"}]
# "METADATA":[]






#Geterate stream defenitions. 
def GenerateStreamDefenitions(Data,SiteIDs):




def ParseCVS(file):
  file = open(file,"r")
  
  df = pandas.read_csv(file)


  print df
  print df['Line']

class InfluxFeedLTSInterface(InfluxDBClient):
  def __init__(self,config_file="influx.json"):

    #Load database credentials
    fp = open(config_file,"r")
    self.config = json.load(fp)
    fp.close()
    
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





if __name__ == "__main__":

  #ParseCVS("testdata/h00t_1310160720_1406030410.csv")
  SiteIDs = LoadSiteIds()

  Feeds = InfluxFeedLTSInterface()

  #Get all data until now + 1h
  StopTime = time.time() + 3600

  for Site in SiteIDs:
      FeedId = "%s/raw_data" % SiteIDs[Site]
      StartTime = Feeds.GetLastTimeStamp(FeedId)
      
      if StartTime == 0:
        print "No previous records in: " + FeedId
        print "Starting from Okt 2013"
        StartTime = time.mktime(time.strptime("2013-10-01","%Y-%m-%d"))
      else:
        print "Last record in stream: " + FeedId
        print "at: " + time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(StartTime))
      
      #Start a tiny bit after the last value.
      Current = StartTime + 0.5
      
      
      PeriodLen = 60*60*24*7
      
      while Current < StopTime:
          print "Reading SLB data from: " + Site 
          print "From: " + time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(Current))
          print "To:   " + time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(Current + PeriodLen))
          Data = ParseSLBData(Site,Current,Current + PeriodLen)

          #Remove duplicate
          if Data["Time"][0] == StartTime:
              Data = Data.drop(Data.index[0])
          
          Current += PeriodLen
          print "Sending data to influx as: " + FeedId

          r = Feeds.SendToInfluxDB(Data,FeedId)
          print "%i Rows written" % r


