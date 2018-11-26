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
import logr as logg
from babel.util import distinct

try:
    import pyspark
except:
    import findspark

    findspark.init()
    import pyspark
# from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def main():
    spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    consumeKafka()


def consumeKafka():
    # spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    try:
        print("Start consuming")
        consumer = KafkaConsumer('data_ingestion', bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',enable_auto_commit=True,group_id=None)#,value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        #consumer = KafkaConsumer('data_ingestion', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')
        #dit_logs
        counter = 0
        for message in consumer:
            print(message)
            print("Inside consumer")
            counter = counter + 1
            print("message counter -------------",counter)
            #message = next(consumer)
            if message.key is not None and message.value is not None:
                print(message)
                value_deserializer = lambda m: json.loads(m.decode('utf-8'))
                data = {'key': message.key, 'value': message.value}
                #data = {'key': message.key, 'value': message.value}
                file = str(message.key.decode('utf-8'))
                print("value----------", message.value)
                print("key----------", file)
                print("data----------"+str(data)+"\n")
                actual_data = str(message.value)+"\n"
                print(actual_data)
                file_path = "C:\\Users\\aj250046\\Documents\\DIT2\\logs\\" + file + ".txt"
                with open(file_path,mode = 'a',encoding = 'utf-8') as f:
                    print("The message is ---------"+actual_data)
                    f.write(actual_data)
            print("End consuming :")

    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in publishKafka() ________________")
        print("Exception::msg %s" % str(e))
        print(traceback.format_exc())


if __name__ == "__main__":
    sys.exit(main())