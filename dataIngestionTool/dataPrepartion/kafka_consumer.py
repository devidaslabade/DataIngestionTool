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
    msg = "End Processing"
    prcId = "PrcId_1"
    consumeKafka()


def consumeKafka():
    # spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    try:
        print("Start consuming")
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
        consumer.subscribe(['test'])
        # for msg in consumer:
        #    print(msg)
        actual_data = []
        for msg in consumer:
            print("Inside consumer")
            message = next(consumer)
            data = {'key': message.key, 'value': message.value}
            file = message.key
            actual_data.append(data)
            print(actual_data)
            f = open("C:\\Users\\aj250046\\Documents\\DIT2\\logs\\file.txt", "w+")

        #consumer.close()
        # KafkaConsumer(consumer_timeout_ms=5000)

        print("End Consuming")

        if len(actual_data) > 0:
            print('Publishing records..')
            for rec in actual_data:
                print("Records--------", rec)

    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in publishKafka() ________________")
        print("Exception::msg %s" % str(e))
        print(traceback.format_exc())


if __name__ == "__main__":
    sys.exit(main())