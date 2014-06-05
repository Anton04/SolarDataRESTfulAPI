import requests
import pandas
import time
import json
from influxdb import InfluxDBClient

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
  
  #Set data keys as column descriptors
  df.columns = col2
  
  #Delete trailing columns with junk. 
  for key in df.keys()[-5:df.shape[1]-1]:
      if key.find(slb_id) != -1:
          del df[key]

  #Reformat timestamps
  for i in range(0,df.shape[0]):
      if df["Time"][i].lower().find("nan") != -1:
          df = df.drop(df.index[i])
          continue
      timestamp = time.mktime(time.strptime(df["Time"][i],"%y-%m-%d %H:%M"))
      df["Time"][i] = timestamp

  return df
  



def ParseCVS(file):
  file = open(file,"r")
  
  df = pandas.read_csv(file)


  print df
  print df['Line']


def SendToInfluxDB(df,FeedId,config_file="influx.json"):
    
    #Series name
    series = FeedId + "/raw_data" 
    
    #Load database credentials
    fp = open(config_file,"r")
    config = json.load(fp)
    fp.close()
    
    #Connect
    client = InfluxDBClient(config["host"], config["port"], config["user"], config["password"], config["database"])
    
    #Save each row
    for i in range(0,Data.shape[0]):
        timestamp = Data.irow(i)[0]
        col = {"time"}
        data = [int(timestamp*1000)]
        
        
        #Remove NANs
        for j in range(1,Data.shape[1]):
            if Data.iloc[i,j] == "NaN" or Data[i][j] == "nan":
                continue
            
            #Add key
            col.append[Data.keys()[j]]
            data.append[Data[i][j]]
            
        fdata = [{
            "points": [data],
            "name": series,
            "columns": col
            }]
    
        client.write_points_with_precision(fdata,"m")

    return

if __name__ == "__main__":

  #ParseCVS("testdata/h00t_1310160720_1406030410.csv")
  SiteIDs = LoadSiteIds()
  
  Data = ParseSLBData(SiteIDs.keys()[0])
  
  print Data.
