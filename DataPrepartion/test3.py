from pyspark.sql import SparkSession
import json
import pandas as pd
from pprint import pprint
from pyspark.sql.types import *
import glob
from glob import iglob
import os, json

if __name__ == "__main__" :
    src = pd.read_json('..\source\src_SrcId_[0-9].json')
    print(src)
    

    '''all_data = pd.DataFrame()
    for f in glob.glob('..\source\src_SrcId_[0-9].json'):
        df = pd.read_json(f)
        all_data = all_data.append(df, ignore_index=True)
        print(all_data[all_data['SrcId']=="SrcId_1"])'''


