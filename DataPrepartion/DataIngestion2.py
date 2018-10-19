from pyspark.sql import SparkSession
import json
import pandas as pd
from pprint import pprint
from pyspark.sql.types import *
#from pyspark.sql.types import StructType, StructField
#from pyspark.sql.types import DoubleType, IntegerType, StringType


def fetchSchema(srcCols):
    print("Fetching schema values of ",srcCols['srcId'].head(1))
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

def launchSpark(srcMap,schemaMap,trgtMap,query):
    spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
    #TODO find alternative to any and restrict it to one row using tail head etc
    ##If source and destination has one entries both side
    for srcKey,src in srcMap.items() :
        print(src)
        if src['SrcType'].any() =='csv' :
            df=spark.read.schema(schema).option("header",src['Header'].any() ).csv(src['SrcLocation'].any())
            df.show()
            df.printSchema()
            for destKey,dest in trgtMap.items() :
                print(query)
                df.selectExpr(query).write.mode('overwrite').format(dest["DestType"].any()).save(dest["DestLocation"].any()+"/"+dest["DestType"].any())
    
    #orc_df.write.orc(















if __name__ == "__main__" :
    src = pd.read_json('..\source\src.json')
    srcColMap = pd.read_json('..\source\srcCols.json')
    dest = pd.read_json('..\dest\dest.json')
    destColMap = pd.read_json('..\dest\destCols.json')
    prc = pd.read_json('..\process\prc.json')
    maps = pd.read_json('..\process\colMapping.json')
    #pprint(prc)
    for prcIdx, prcRow in prc[prc['isActive']=="True"].iterrows():
        query=[]
        schemaMap={}
        srcMap={}
        destMap={}
        mapTab=maps[maps['mapId']==prcRow['mapId']]
        #print(mapTab)
        for mapId,mapRow in mapTab.iterrows() :
            srcCol=srcColMap[(srcColMap['srcId']== mapRow['srcId']) & (srcColMap['colId']== mapRow['srcColId'])]
            destCol=destColMap[(destColMap['destId']== mapRow['destId']) & (destColMap['colId']== mapRow['destColId'])]
            query.append(srcCol['colName'].str.cat()+" as "+destCol['colName'].str.cat())
            ##Fetch schema of the sources
            if mapRow['srcId'] in schemaMap:
                print("skipping the block")
            else :
                fields=fetchSchema(srcColMap[srcColMap['srcId']== mapRow['srcId']])             
                schema = StructType(fields)
                schemaMap[mapRow['srcId']]=schema
                srcMap[mapRow['srcId']]=src[src['srcId']==mapRow['srcId']]
                destMap[mapRow['destId']]=dest[dest['destId']==mapRow['destId']]
        launchSpark(srcMap,schemaMap,destMap,query)        
