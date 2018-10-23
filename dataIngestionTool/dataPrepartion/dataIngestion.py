import sys
import json
import glob
import pandas as pd
from pprint import pprint
try:
    import pyspark
except:
    import findspark
    findspark.init()
    import pyspark
#from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import *
#from pyspark.sql.types import StructType, StructField
#from pyspark.sql.types import DoubleType, IntegerType, StringType
from configparser import ConfigParser
# instantiate
config = ConfigParser()

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
    spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    #TODO find alternative to any and restrict it to one row using tail head etc
    ##If source and destination has one entries both side
    for srcKey,src in srcMap.items() :
        print(srcKey)
        print(type(srcKey))
        if src['fileType'].any() == "csv" or src['fileType'].any() == "json" or src['fileType'].any() == "parquet" or src['fileType'].any() == "orc":
            df = spark.read.schema(schemaMap[srcKey]).option("header",src['header'].any()).csv(src['srcLocation'].any())
            df.show()
            df.printSchema()
        elif src['fileType'].any() == "hivetable":
            print(src["table"].any())
            colName = ','.join(schemaMap[srcKey].fieldNames())
            df = spark.sql('SELECT '  + colName + ' FROM ' + src["table"].any())
            df.show()
            #df = spark.read.schema(schemaMap[srcKey]).option("header", src['header'].any()).csv(src['srcLocation'].any())
            #df.write.format("csv").saveAsTable("categories")


        for destKey,dest in trgtMap.items() :
                print(query)
                print(type(src['fileType'] ))
                print(src['fileType'] )
                if dest['fileType'].any() == "csv" or dest['fileType'].any() == "json" or dest['fileType'].any() == "orc" or dest['fileType'].any() == "parque" :
                    print("IN If")
                    df.selectExpr(query).write.mode(dest["mode"].any()).format(dest["fileType"].any()).save(dest["destLocation"].any()+"/"+dest["fileType"].any())
                elif dest['fileType'].any() == "hivetable":
                    print("IN else")
                    df.write.mode(dest["mode"].any()).saveAsTable(dest["table"].any())
















def main(configPath,args):
    # parse existing file
    config.read(configPath)
    #Read Process files in iteration
    for prcFile in glob.glob(config.get('DIT_setup_config', 'prcDetails')+'prc_PrcId_[0-9].json'):
        prc = pd.read_json(prcFile)
        for prcIdx, prcRow in prc[prc['isActive']=="True"].iterrows():
            query=[]
            schemaMap={}
            srcMap={}
            destMap={}
            #Fetch process Id specific mapping file
            maps = pd.read_json(config.get('DIT_setup_config', 'prcMapping')+'colMapping_'+prcRow['mapId']+'.json')
            mapTab=maps[maps['mapId']==prcRow['mapId']]
            #print(mapTab)
            for mapId,mapRow in mapTab.iterrows() :
                #Fetch source and destination column mapping files
                srcColMap = pd.read_json(config.get('DIT_setup_config', 'srcCols')+'srcCols_'+mapRow['srcId']+'.json')
                destColMap = pd.read_json(config.get('DIT_setup_config', 'destCols')+'destCols_'+mapRow['destId']+'.json')
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
                    #Fetch source and destination details
                    src = pd.read_json(config.get('DIT_setup_config', 'srcDetails')+'src_'+mapRow['srcId']+'.json')
                    dest = pd.read_json(config.get('DIT_setup_config', 'destDetails')+'dest_'+mapRow['destId']+'.json')
                    srcMap[mapRow['srcId']]=src[src['srcId']==mapRow['srcId']]
                    destMap[mapRow['destId']]=dest[dest['destId']==mapRow['destId']]
            launchSpark(srcMap,schemaMap,destMap,query)        


if __name__ == "__main__" :
    #sys.exit(main('C:\\Users\\sk250102\\Documents\\Teradata\\DIT\\DataIngestionTool\\config\\config.cnf',sys.argv))
    sys.exit(main('C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\config\\config.cnf', sys.argv))