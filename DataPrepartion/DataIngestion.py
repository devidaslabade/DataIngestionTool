from pyspark.sql import SparkSession
import json
from pprint import pprint
from pyspark.sql.types import *
#from pyspark.sql.types import StructType, StructField
#from pyspark.sql.types import DoubleType, IntegerType, StringType


def fetchSchema(srcCols):
    print("Fetching schema values of ",srcCols)
    fields=[]
    for clm in srcCols :
        print(clm['isNullable'])
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
    spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
    if src['SrcType'] == "csv" :
        df=spark.read.schema(schema).option("header",src['Header'] ).csv(src['SrcLocation'])
        df.show()
        df.printSchema()
        print(trgt)
        print(trgt["DestLocation"]+"/"+trgt["DestType"])
        df.write.mode('overwrite').format(trgt["DestType"]).save(trgt["DestLocation"]+"/"+trgt["DestType"])
    
    #orc_df.write.orc(















if __name__ == "__main__" :
    src = json.loads(open('..\source\src.json', encoding='utf-8').read())
    srcColMap = json.loads(open('..\source\srcCols.json', encoding='utf-8').read())
    dest = json.loads(open('..\dest\dest.json', encoding='utf-8').read())
    prc = json.loads(open('..\process\prc.json', encoding='utf-8').read())
    #pprint(json)
    for prcKey, prcVal in prc.items():
        print(prcVal['PrcName'])
        #fields = [StructField(x['colName'], StringType(), False) for x in srcColMap[key]]
        fields=fetchSchema(srcColMap[prcVal['SrcId']])             
        schema = StructType(fields)
        launchSpark(src[prcVal['SrcId']],schema,dest[prcVal['DestId']])        
        #launchSpark(src[value['SrcId']]['SrcLocation'],schema,dest[value['TrgId']])

    