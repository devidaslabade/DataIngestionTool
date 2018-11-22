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

    spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    msg = "Start Processing"
    prcId = "PrcId_12"
    publishKafka(spark,prcId,msg)
    
def publishKafka(spark, prcId,msg):
    # spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    app_id = "123"
    app_name = "local"
    try:
        print("Start publishKafka")
        fmt = "%Y-%m-%d"
        current_date = str(datetime.datetime.now().strftime(fmt))
        jsonString = {"prc_id" : prcId ,"isException": "true","msg":msg}
        prcKey = str(prcId+"-"+app_name+"-"+app_id+"-"+current_date)
        print(prcKey)
        producer = KafkaProducer(bootstrap_servers='localhost:9092',key_serializer=str.encode,value_serializer=lambda v: json.dumps(jsonString).encode('utf-8'))
        producer.send('test', key=prcKey, value=b'jsonString')
        #producer.send('test', jsonString)
        print("End of publishKafka")

        print("Start consuming")
        #consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
        #consumer.subscribe(['test'])
        consumer = KafkaConsumer('test', auto_offset_reset='earliest',
                                 bootstrap_servers=['localhost:9092'], consumer_timeout_ms=1000)

        consumer.poll()
        # for msg in consumer:
        #    print(msg)
        actual_data = []
        for msg in consumer:
            print("Inside consumer for loop")
            message = next(consumer)
            data = {'key': msg.key, 'value': msg.value}
            print("name of the file ----------"+prcKey)
            actual_data.append(data)
            print(actual_data)
            print("prcKey-------------"+prcKey)
            print("Message-------------" , msg.value)
            file_path = "C:\\Users\\aj250046\\Documents\\DIT2\\logs\\"+prcKey+".txt"
            if os.path.exists(file_path):
                print("----------------file exist------------")
                append_write = 'a'  # append if already exists
            else:
                print("-----------------file does not exist----------")
                append_write = 'w+'  # make a new file if not

            print("append_write---------------" + append_write)
            f = open(file_path,append_write)
            f.write(str(data))
        consumer.close()
        print("End consuming")

    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in publishKafka() ________________")
        print("Exception::msg %s" % str(e))
        print(traceback.format_exc())


if __name__ == "__main__" :
    sys.exit(main())