# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>


import pandas as pd
import InfluxDBInterface
import time
from ElasticsearchInterface import ESinterface


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
        
        df.loc[site,"OpID"] = res[site]["Operator_ID"]
        
        df.loc[site,"LogStart"] = LogDB.GetFirstTimestamp(site)
        df.loc[site,"LogStop"] = LogDB.GetLastTimestamp(site)
        
        df.loc[site,"ProdStart"]  = ProductionDB.GetFirstTimestamp(site) 
        df.loc[site,"ProdStop"] = ProductionDB.GetLastTimestamp(site)
    
    
    df["LogLag"] = now - df["LogStop"]
    df["ProdLag"] = now - df["ProdStop"]
    
    print "\rDone!           "
    
    
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

def UpdateStatusWebpage(filename = "/var/www/html/status.html"):
    
    status_raw = GetRawStatusFrame()
    status = FormatStatusFrame(status_raw)
    
    html = status.to_html(max_rows=40)
    
    file = open(filename,"w")
    
    file.write(html)
    
    file.close()
    
    #print status
    
    print "Status Written to %s" % filename
    

# <codecell>

if __name__ == '__main__':


    DataLink = InfluxDBInterface.InfluxDBInterface("influxInterfaceCredentials2.json")

    LogDB = DataLink.databases[u'SolarLogdata']
    ProductionDB = DataLink.databases[u'SolarProductionSites']
    AreaDB = DataLink.databases[u'SolarProductionAreas']
    Test = DataLink.databases[u'test']

    es = ESinterface()

    UpdateStatusWebpage()


# <codecell>


