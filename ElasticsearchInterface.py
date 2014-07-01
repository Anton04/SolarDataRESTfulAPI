#!/usr/bin

import pandas
from influxdb import InfluxDBClient
import json
import numpy
from elasticsearch import Elasticsearch

class ESinterface(Elasticsearch):
  def SaveDataFrameAs(self,index,type,id_param,dataframe):
    pass
  
