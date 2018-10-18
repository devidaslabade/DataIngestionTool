from pyspark.sql import SparkSession
import json
import pandas as pd
from pprint import pprint
from pyspark.sql.types import *
#from pyspark.sql.types import StructType, StructField
#from pyspark.sql.types import DoubleType, IntegerType, StringType


def fetchSchema(srcCols):
    print("Fetching schema values of ",srcCols)
    fields=[]
    for idx,clm in srcCols.iterrows() :
        if clm['colType'] == "String" :
            colField =StructField(clm['colName'], StringType(), eval(clm['isNullable']))
            fields.append(colField)
        elif clm['colType'] == "Integer" :
            colField =StructField(clm['colName'], IntegerType(), eval(clm['isNullable']))
            fields.append(colField)
        else :
            colField =StructField(clm['colName'], StringType(), eval(clm['isNullable']))
            fields.append(colField)
    return fields

def launchSpark(src,schema,trgt):
    print(type(src))
    print(type(src['SrcType']=='csv'))
    print(type(schema))
    print(schema)
    print(type(trgt))
    print(trgt['DestLocation'])
    spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
    #TODO find alternative to any and restrict it to one row using tail head etc
    if src['SrcType'].any() =='csv' :
        df=spark.read.schema(schema).option("header",src['Header'].any() ).csv(src['SrcLocation'].any())
        df.show()
        df.printSchema()
        print(trgt)
        #print(trgt["DestLocation"]+"/"+trgt["DestType"])        
        df.write.mode('overwrite').format(trgt["DestType"].any()).save(trgt["DestLocation"].any()+"/"+trgt["DestType"].any())
    
    #orc_df.write.orc(















if __name__ == "__main__" :
    src = pd.read_json('..\source\src.json')
    srcColMap = pd.read_json('..\source\srcCols.json')
    dest = pd.read_json('..\dest\dest.json')
    prc = pd.read_json('..\process\prc.json')
    #pprint(prc)
    for prcIdx, prcRow in prc.iterrows():
        #fields = [StructField(x['colName'], StringType(), False) for x in srcColMap[key]]
        fields=fetchSchema(srcColMap[srcColMap['SrcId']==prcRow['SrcId']])             
        schema = StructType(fields)
        launchSpark(src[src['SrcId']==prcRow['SrcId']],schema,dest[dest['DestId']==prcRow['DestId']])        
        
        
        
        #launchSpark(src[value['SrcId']]['SrcLocation'],schema,dest[value['TrgId']])

    