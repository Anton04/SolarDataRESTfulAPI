#!/usr/bin/python
# -*- coding: utf-8 -*-

#Add path to API files. 
import sys
sys.path.append("/home/iot/repos/SolarDataRESTfulAPI")

import pandas as pd
import InfluxDBInterface
import time
from ElasticsearchInterface import ESinterface
import argparse

def EpocToDate(timestamp):
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(timestamp))
    except:
        return "-"

def SecToHMS(sec):
    try:
        sec = int(sec)
        hour = sec / 3600
        minutes = (sec - (hour * 3600))/60
        secs = sec % 60
        return "%i h %i min %i s" %(hour,minutes,secs)
    except:
        return "-"

# <codecell>

def GetRawStatusFrame():

    #Sites = LogDB.ListSeries()
    res = es.GetHitsAsDataFrame(index="solar-sites-index", doc_type='meta-data')
    
    Sites = res.columns
    
    print "Procssing"
    
    
    
    df = pd.DataFrame()
    df.index.name = "Sites"
    
    now = time.time() *1000
    
    for site in Sites:
        print "*",
        sys.stdout.flush()
        
        df.loc[site,"OpID"] = res[site]["Operator_ID"]
        
        df.loc[site,"LogStart"] = LogDB.GetFirstTimestamp(site)
        df.loc[site,"LogStop"] = LogDB.GetLastTimestamp(site)
        
        df.loc[site,"ProdStart"]  = ProductionDB.GetFirstTimestamp(site) 
        df.loc[site,"ProdStop"] = ProductionDB.GetLastTimestamp(site)
    
    
    df["LogLag"] = now - df["LogStop"]
    df["ProdLag"] = now - df["ProdStop"]
    
    print "\rDone!                "
    sys.stdout.flush()
    
    
    return df

# <codecell>

def FormatStatusFrame(df):
    df2 = pd.DataFrame()
    
    df2["OpID"] = df["OpID"]
    
    df2["LogLag"] = (df.LogLag/1000).apply(SecToHMS)
    df2["LogStop"] = (df.LogStop/1000).apply(EpocToDate)
    df2["LogStart"] = (df.LogStart/1000).apply(EpocToDate)
    
    df2["ProdLag"] = (df.ProdLag/1000).apply(SecToHMS)
    df2["ProdStop"] = (df.ProdStop/1000).apply(EpocToDate)
    df2["ProdStart"] = (df.ProdStart/1000).apply(EpocToDate)
    
    df2 = df2.reindex_axis(df.columns, axis=1)
    
    return df2

# <codecell>

def GetStatusFrame():
    return FormatStatusFrame(GetRawStatusFrame())

# <codecell>

def UpdateStatusWebpage(filename = "/var/www/html/status_slb.html"):
    
    status_raw = GetRawStatusFrame()
    status = FormatStatusFrame(status_raw.sort("LogLag"))
    
    html = status.to_html(max_rows=40)
    
    file = open(filename,"w")
    
    file.write(html)
    
    file.close()
    
    #print status
    
    print "Status Written to %s" % filename
    

# <codecell>

if __name__ == '__main__':

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-c', dest='credentials_file', default="/home/iot/repos/SolarDataRESTfulAPI/influxInterfaceCredentials2.json", help='Credential file')
    parser.add_argument('-d', dest='database', default="mqtt", help='database')
    parser.add_argument('-s', dest='series', default="", help='series')
    parser.add_argument('-p', dest='power_key', default="power", help='name of power key')
    parser.add_argument('-e', dest='energy_key', default="energy", help='name of energy key')

    args = parser.parse_args()

    DataLink = InfluxDBInterface.InfluxDBInterface(args.credentials_file)

    #LogDB = DataLink.databases[u'SolarLogdata']
    #ProductionDB = DataLink.databases[u'SolarProductionSites']
    #AreaDB = DataLink.databases[u'SolarProductionAreas']
    
    DataBase = DataLink.databases[args.database]

    #es = ESinterface()

    #UpdateStatusWebpage()
    (time_p,power) = DataBase.GetLastValue(args.series,[arg.power_key])
    (time_e,energy) = DataBase.GetLastValue(args.series,[arg.energy_key])
    
    

# <codecell>


