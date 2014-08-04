#!/usr/bin/python
# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

#cd git/SolarDataRESTfulAPI/

# <codecell>

import json
import pandas as pd
import InfluxDBInterface
import time
reload(InfluxDBInterface)
from ElasticsearchInterface import ESinterface
import sys


DataLink = InfluxDBInterface.InfluxDBInterface("influxInterfaceCredentials2.json")

LogDB = DataLink.databases[u'SolarLogdata']
ProductionDB = DataLink.databases[u'SolarProductionSites']


es = ESinterface()


# <codecell>



def CalculateProduction(LogDB,ProductionDB):
    
    aWeek = 7*60*60*24
    
    Sites = LogDB.ListSeries()
    
    for Site in Sites:
        CalculateEnergyCounterForSite(LogDB,ProductionDB,Site,aWeek)
    
            
            
            
            
            
def CalculatePowerForSite(LogDB,ProductionDB,Site,PeriodSize):
    print "Processing Power for %s" % Site
    Properties = LogDB.GetPropertiesPartiallyMatchingAbutNotB(Site,"Pac","Tot")
    print "\t%i inverters found" % len(Properties)
    
    LastUpdate = ProductionDB.GetLastTimestamp(Site,"Power")
    
    #No previous calculations done, start from the beginnnig of log series. 
    if LastUpdate == None:
        print "\tNo previous power data calculated for %s, starting from beginning." % Site
        LastUpdate = LogDB.GetFirstTimestamp(Site)
        
        #No data.
        if LastUpdate == None:
            print "\tNo data found for %s" % Site
            return
    
    else:
        print "\tStarting calculations from: %i" % LastUpdate
        
    DataUntil = LogDB.GetLastTimestamp(Site)
    PeriodStart = LastUpdate
    
    if DataUntil == PeriodStart:
        print "\tUp to date!"
    
    #Loop trough timeseries
    while PeriodStart < DataUntil:
        df = LogDB.GetDataPeriod(Site,Properties,PeriodStart/1000,PeriodSize,10000)
        if type(df) != pd.core.frame.DataFrame:
            print "Missing data at: %i" % PeriodStart
            PeriodStart += PeriodSize*1000
            continue
            
        SumColsIntoCol(df,Properties,"Power")
        row = ProductionDB.Save(Site,df[["Power"]])
        print "\t%i rows of data saved to %s" % (row,Site)
        PeriodStart += PeriodSize*1000
        
def CalculateEnergyCounterForSite(LogDB,ProductionDB,Site,PeriodSize):
    print "Processing Energy counter for %s" % Site
    Properties = LogDB.GetPropertiesPartiallyMatchingAbutNotB(Site,"POWc","Tot")
    print "\t%i inverters found" % len(Properties)
    
    (LastUpdate,LastValue) = ProductionDB.GetLastValue(Site,"Energy")
    
    
    
    #No previous calculations done, start from the beginnnig of log series. 
    if LastUpdate == None:
        print "\tNo previous energy data calculated for %s, starting from beginning." % Site
        
        #Start from where we have raw data
        LastUpdate = LogDB.GetFirstTimestamp(Site)
        
        #Counter start from 0 
        LastValue = 0
        
        #No data.
        if LastUpdate == None:
            print "\tNo data found for %s" % Site
            return
    
    else:
        print "\tStarting calculations from: %s" % EpocToDate(LastUpdate/1000)
        
    DataUntil = LogDB.GetLastTimestamp(Site)
    PeriodStart = LastUpdate
    
    if DataUntil == PeriodStart:
        print "\tUp to date!"
    
    #Loop trough timeseries
    while PeriodStart < DataUntil:
        print "\tRunning period %s to %s" % (EpocToDate(PeriodStart/1000),EpocToDate((PeriodStart/1000+PeriodSize)))
        df = LogDB.GetDataPeriod(Site,Properties,PeriodStart/1000,PeriodSize,10000)
        if type(df) != pd.core.frame.DataFrame:
            print "\t\tNo data"
            PeriodStart += PeriodSize*1000
            continue
        else:
            print "\t\t%i rows of data found." % df.shape[0]
        
        #Remove the reset every 24h.
        df = df.apply(RemoveResets)
        
        #We need a continious series 
        if df.index[0]*1000 != LastUpdate:
            print "\t*** Sync error"
            
            print df.index[0]*1000 , LastUpdate
            print df.index[-1]*1000
            print df.shape
            print PeriodStart
            break
        
        SumColsIntoCol(df,Properties,"Energy")
        
        #Add previous counter value 
        Offset = LastValue - df["Energy"].iloc[0]
        df["Energy"] += Offset
        
        LastUpdate = df.index[-1]*1000
        LastValue = df["Energy"].iloc[-1]
        
        #Drop duplicate row and save if data.
        if df.shape[0] > 1:
            row = ProductionDB.Save(Site,df.iloc[1:][["Energy"]])
            print "\t%i rows of data saved to %s" % (row,Site)
            
        PeriodStart += PeriodSize*1000
        
    print "\tEnergy calculations finnished!"
                
def SumColsIntoCol(df,Properties,Name):
    df[Name] = 0
    
    for p in Properties:
        df[Name] += df[p]
    
                




# <codecell>

def EpocToDate(timestamp):
    return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(timestamp))
        
def SecToHMS(sec):
    sec = int(sec)
    hour = sec / 3600
    minutes = (sec - (hour * 3600))/60
    secs = sec % 60
    return "%i h %i min %i s" %(hour,minutes,secs)

def RemoveResets(series):
    
    FirstValue = series.iloc[0]
    change =  series.diff().clip(0)
    change.iloc[0] = FirstValue
    return change.cumsum()

def CalculateProduction(Site,LogDB,ProductionDB,Recalculate=False):

    #Create property lists
    EnergyProp = LogDB.GetPropertiesPartiallyMatchingAbutNotB(Site,"POWc","Tot")
    PowerProp = LogDB.GetPropertiesPartiallyMatchingAbutNotB(Site,"Pac","Tot")
    
    PreviousLastValidValue = 0
    PreviousLastValidValueTime = 0
    
    #Determine where to resume.
    if Recalculate == False:
        (PreviousLastValidValueTime,PreviousLastValidValue) = ProductionDB.GetLastValue(Site,"Energy")
        TimestampP = ProductionDB.GetLastTimestamp(Site,"Power")
        
        #The start from where we have both power and energy values. 
        if TimestampP < PreviousLastValidValueTime:
            PreviousLastValidValueTime = TimestampP
            
        PreviousLastValidValueTime = PreviousLastValidValueTime / 1000
            
        print "\tResuming calculation from: %s" % EpocToDate(PreviousLastValidValueTime)
            
        #Get last data. 
        dfLog = LogDB.GetDataAfterTime(Site,EnergyProp + PowerProp,PreviousLastValidValueTime,100)
    
    else:  
        #Get a log data chunck
        dfLog = LogDB.GetDataAfterTime(Site,EnergyProp + PowerProp,None,100)
    
    while (dfLog.shape[0] > 1):
    
        #Create a production frame.
        dfProduction = pd.DataFrame(columns = ["Power","Energy"])
        
        
        #Calculate power
        dfProduction["Power"] = dfLog[PowerProp].sum(axis=1)
        
        #Calculate energy
        dfPOWc = dfLog[EnergyProp]
        dfProduction["Energy"] = dfPOWc.apply(RemoveResets).sum(axis=1)
        
        #Add offset from previus iteration.
        
        #Check if we have overlap. Is the last time the same as the smallest countervalue in the current array.
        FirstValidValueTime = dfProduction["Energy"].idxmin()
        
        #First time ever... or just NaN values in data. 
        if PreviousLastValidValueTime == None or pd.isnull(FirstValidValueTime):
            offset = 0
        #Normal overlap
        else:   
            offset = PreviousLastValidValue - dfProduction["Energy"][FirstValidValueTime]
        
        dfProduction["Energy"] += offset
        
        #Update database
        ProductionDB.Replace(Site,dfProduction)
        
        #Keep track of counter max.
        MaxEnergyTime = dfProduction["Energy"].idxmax()
        
        if not pd.isnull(MaxEnergyTime):
            PreviousLastValidValue = dfProduction["Energy"][MaxEnergyTime]
            PreviousLastValidValueTime = MaxEnergyTime
        
        dfLog = LogDB.GetNextNRows(dfLog,1000)
        
    return dfLog.index[-1]

# <codecell>

if __name__ == '__main__':


    Sites = LogDB.ListSeries()

    now = time.time()
        
    for Site in Sites:
     
    #Site = "46d55815-f927-459f-a8e2-8bbcd88008ee"
        print "Processing %s " % Site 
        
        sys.stdout.flush()

        until = CalculateProduction(Site,LogDB,ProductionDB,False)
        
        until = int(now - until)
        
        hour = until / 3600
        
        minutes = (until - (hour * 3600))/60
        
        secs = until % 60
        
        print "\tFinnished processing up to %i hours %i minutes and %i seconds from script start time" % (hour,minutes,secs)
        
        sys.stdout.flush()

    print "Done"

    sys.stdout.flush()


