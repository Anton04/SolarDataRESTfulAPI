import requests
import pandas
import time

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
    
    
    
def ParseSLBData(id="h00t",start,stop):
  id = "h00t"
  
  
  
  starttime = time.strftime("%y%m%d%H%M",start)
  stoptime = time.strftime("%y%m%d%H%M",stop)
  url = "http://slb.nu/soldata/index.php?KEY=%s&start=%i&stop=%i" %(id,starttime,stoptime)

  df = pandas.read_csv(url,sep = ";",parse_dates=[[0, 1]],skiprows=8, header = None ,infer_datetime_format = True,na_values = ["     ","    ","  "," ",""])
  cl = pandas.read_csv(url,sep = ";", header = 6,error_bad_lines= False,na_values = [""],nrows=1)

  #Align keys to data. 
  cols = cl.keys()
  cols = cols[1:]
  col2 = cols.insert(-1,"NAN")
  
  #Set data keys as column descriptors
  df.columns = col2

  return df
  



def ParseCVS(file):
  file = open(file,"r")
  
  df = pandas.read_csv(file)


  print df
  print df['Line']




if __name__ == "__main__":

  ParseCVS("testdata/h00t_1310160720_1406030410.csv")
  
  
