#!/usr/bin

import pandas
from influxdb import InfluxDBClient
import json
import numpy
from elasticsearch import Elasticsearch

class ESinterface(Elasticsearch):
  def SaveDataFrameAsIndex(self,index,doc_type,df,clear = False):
    
    
    #Delete all old entries?
    if clear:
      try:
        self.indices.delete(index)
      except:
        pass
      
    #Iterate through all rows.  
    for item in df.index:
    
      column = []
      data = []
      docs = {}
      
      if type(item) != type(""):
          #print "is nan"
          continue
          
      print "Processing meta for site: " + item
      
      #Iterate through all properties
      for Key in df.keys():
          
        Value = df.loc[item][Key]
        
        #Skip if value is missing
        if type(Value) != type("") and numpy.isnan(Value):
            continue
            
        #Add value
        column.append(Key)
        data.append(Value)
        docs[Key] = Value
        
        #Add time if avalible.
        if Key == "online_since":
            column.append("time")
            data.append(Value)
            
            print "Added time key"
      
      #Send data to elastic search
      res = self.index(index=index, doc_type=doc_type, id=item, body=docs)
      if (res['created']):
          print "Created elasticsearch entry"
      else:
          print "Updated elasticsearch entry"
  
  #Update indices 
    self.indices.refresh(index=index)
    
