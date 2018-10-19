import findspark
findspark.init()
from pyspark.sql import SparkSession
import json
import pandas as pd
import sys
import glob
from glob import iglob
import os, json


def main(args):
    print(args)    
    fpa="C:\\Users\\sk250102\\Documents\\Teradata\\DIT\\DataIngestionTool\\config\\source\\src_SrcId_1.json"
    src = pd.read_json(fpa)
    #src = pd.read_json('..\..\config\source\src_SrcId_1.json')
    print(src)
    

    '''all_data = pd.DataFrame()
    for f in glob.glob('..\source\src_SrcId_[0-9].json'):
        df = pd.read_json(f)
        all_data = all_data.append(df, ignore_index=True)
        print(all_data[all_data['SrcId']=="SrcId_1"])'''


if __name__ == "__main__" :
    sys.exit(main(sys.argv))