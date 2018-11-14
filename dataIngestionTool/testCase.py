
from array import array
from glob import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
import random
import threading
import hashlib

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest
    if sys.version_info[0] >= 3:
        xrange = range
        basestring = str

SPARK_HOME = os.environ["SPARK_HOME"]


_have_scipy = False
_have_numpy = False
try:
    import scipy.sparse
    _have_scipy = True
except:
    # No SciPy, but that's okay, we'll skip those tests
    pass
try:
    import numpy as np
    _have_numpy = True
except:
    # No NumPy, but that's okay, we'll skip those tests
    pass
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

sys.path.append('../')


try:
    import pyspark
except:
    import findspark

    findspark.init()
    import pyspark
#from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import *
# from pyspark.sql.types import StructType, StructField
# from pyspark.sql.types import DoubleType, IntegerType, StringType
from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def test_csv():
    schema = StructType([
        StructField("category_id", IntegerType()),
        StructField("category_department_id", DoubleType()),
        StructField("category_name", StringType())
    ])

    actualDF = pd.read_csv("C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\srcLoc\\category.csv")
    #spark = SparkSession.builder.master("local").appName("Data Ingestion").config("spark.some.config.option",
             #                                                                     "some-value").getOrCreate()
    #actualDF = spark.read.schema(schema).option("header", "true").csv("C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\srcLoc\\category.csv").load()
    #actualDF = spark.read.csv("C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\srcLoc\\category.csv")
    print("hello")
    actualDFCount = actualDF.count()
    print("Actual ----", actualDFCount)
    observedDF = pd.read_csv("C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\trgLoc\\")

    print("Observed    -----", observedDF.count())


if __name__ == "__main__":
    from pyspark.tests import *
    test_csv()


