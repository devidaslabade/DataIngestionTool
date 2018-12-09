import os
import re
import sys
import json
import glob
from multiprocessing.dummy import Pool as ThreadPool
from itertools import repeat
import pandas as pd
import datetime
import traceback
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from kafka import KafkaConsumer
from time import sleep

try:
    import pyspark
except:
    import findspark

    findspark.init()
    import pyspark
#from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def main():

    spark = "LocalJob"
    msg = "Mid of  Processing"
    prcId = "PrcId_2"
    publishKafka(spark,prcId,msg)
    
def publishKafka(spark, prcId,msg):
    # spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    app_id = "123"
    app_name = "local"
    try:
        print("Start publishKafka")
        fmt = "%Y-%m-%d"
        current_date = str(datetime.datetime.now().strftime(fmt))
        jsonString = {"prc_id": prcId ,"isException": "true","msg":msg}
        prcKey = str(prcId+"-"+app_name+"-"+app_id+"-"+current_date)
        print(prcKey)
        producer = KafkaProducer(bootstrap_servers=['10.0.75.1:9092'])
        #producer = KafkaProducer(bootstrap_servers='10.0.75.1:9092',value_serializer=lambda v: json.dumps(jsonString).encode('utf-8'))
        producer.send('test', key=prcKey.encode('utf-8'), value=json.dumps(jsonString).encode('utf-8'))

        producer.flush()
        print("End of publishKafka")

    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in publishKafka() ________________")
        print("Exception::msg %s" % str(e))
        print(traceback.format_exc())

if __name__ == "__main__" :
    sys.exit(main())