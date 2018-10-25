import sys
import json
import glob
from multiprocessing.dummy import Pool as ThreadPool
from itertools import repeat
import pandas as pd
import datetime
sys.path.append('../')
import comnUtil.logr as logg
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



def prepareMeta(sprkSession,prcRow):
    spark_logger = logg.Log4j(sprkSession,prcRow['prcId'])            
    spark_logger.warn("_________________Started processing process Id : " +prcRow['prcId']+" : ____________________") 
    query=[]
    schemaMap={}
    srcMap={}
    destMap={}
    #Fetch process Id specific mapping file
    maps = pd.read_json(config.get('DIT_setup_config', 'prcMapping')+'colMapping_'+prcRow['mapId']+'.json')
    mapTab=maps[maps['mapId']==prcRow['mapId']]
    for mapId,mapRow in mapTab.iterrows() :
        #Fetch source and destination column mapping files
        srcColMap = pd.read_json(config.get('DIT_setup_config', 'srcCols')+'srcCols_'+mapRow['srcId']+'.json')
        destColMap = pd.read_json(config.get('DIT_setup_config', 'destCols')+'destCols_'+mapRow['destId']+'.json')
        srcCol=srcColMap[(srcColMap['srcId']== mapRow['srcId']) & (srcColMap['colId']== mapRow['srcColId'])]                
        destCol=destColMap[(destColMap['destId']== mapRow['destId']) & (destColMap['colId']== mapRow['destColId'])]
        #query.append(srcCol['colName'].str.cat()+" as "+destCol['colName'].str.cat())
        query.append("cast("+srcCol['colName'].str.cat()+" as "+destCol['colType'].str.cat()+" ) as "+destCol['colName'].str.cat())
        ##Fetch schema of the sources
        if mapRow['srcId'] not in schemaMap:
            fields=fetchSchema(srcColMap[srcColMap['srcId']== mapRow['srcId']],spark_logger)             
            schema = StructType(fields)
            schemaMap[mapRow['srcId']]=schema
            #Fetch source and destination details
            src = pd.read_json(config.get('DIT_setup_config', 'srcDetails')+'src_'+mapRow['srcId']+'.json')
            dest = pd.read_json(config.get('DIT_setup_config', 'destDetails')+'dest_'+mapRow['destId']+'.json')
            srcMap[mapRow['srcId']]=src[src['srcId']==mapRow['srcId']]
            destMap[mapRow['destId']]=dest[dest['destId']==mapRow['destId']]
        processData(sprkSession,srcMap,schemaMap,destMap,query,spark_logger)
    








def fetchSchema(srcCols,spark_logger):
    spark_logger.warn("Fetching schema values for SRC Id "+srcCols['srcId'].str.cat())
    fields=[]
    for idx,clm in srcCols.iterrows() :
        if clm['colType'].lower() == "String".lower() :
            colField =StructField(clm['colName'], StringType(), eval(clm['isNullable']))
            fields.append(colField)
        elif clm['colType'].lower() == "Int".lower() :
            colField =StructField(clm['colName'], IntegerType(), eval(clm['isNullable']))
            fields.append(colField)
        elif clm['colType'].lower() == "Float".lower() :
            colField =StructField(clm['colName'], FloatType(), eval(clm['isNullable']))
            fields.append(colField)
        elif clm['colType'].lower() == "Double".lower() :
            colField =StructField(clm['colName'], DoubleType(), eval(clm['isNullable']))
            fields.append(colField)    
        elif clm['colType'].lower() == "Boolean".lower() :
            colField =StructField(clm['colName'], BooleanType(), eval(clm['isNullable']))
            fields.append(colField)
        else :
            colField =StructField(clm['colName'], StringType(), eval(clm['isNullable']))
            fields.append(colField)
    return fields

def processData(spark,srcMap,schemaMap,trgtMap,query,spark_logger):
   
    #TODO find alternative to any and restrict it to one row using tail head etc
    ##If source and destination has one entries both side
    for srcKey,src in srcMap.items() :
        try:
            if src['fileType'].any() == "csv" or src['fileType'].any() == "json" or src['fileType'].any() == "parquet" or src['fileType'].any() == "orc":
                df = spark.read.schema(schemaMap[srcKey]).option("header",src['header'].any()).csv(src['srcLocation'].any())
                df.show()
                df.printSchema()
            elif src['fileType'].any() == "hivetable":
                colName = ','.join(schemaMap[srcKey].fieldNames())
                df = spark.sql('SELECT '  + colName + ' FROM ' + src["table"].any())
                df.show()
            elif src['fileType'].any() == "jdbcclient":
                print(src["table"].any())
                df = spark.read.format("jdbc").option("url", src["url"].any()).option("driver", src["driver"].any()).option("dbtable", src["table"].any()).option("user", src["user"].any()).option("password",src["password"].any()).load()
                df.show()    
        except Exception as e:
            print (str(datetime.datetime.now()) + "____________ Exception occurred in processData() ________________")
            print(str(datetime.datetime.now()) + " The iteration key for srcMap is :: "+srcKey)
            raise Exception("Exception::msg %s" %str(e)) 
            #df = spark.read.schema(schemaMap[srcKey]).option("header", src['header'].any()).csv(src['srcLocation'].any())
            #df.write.format("csv").saveAsTable("categories")


        for destKey,dest in trgtMap.items() :
                print(query)
                try:
                    if dest['fileType'].any() == "csv" or dest['fileType'].any() == "json" or dest['fileType'].any() == "orc" or dest['fileType'].any() == "parque" :
                        df.selectExpr(query).write.mode(dest["mode"].any()).format(dest["fileType"].any()).save(dest["destLocation"].any()+dest["destId"].any()+"_"+dest["fileType"].any()+"/" +dest["fileType"].any())
                    elif dest['fileType'].any() == "hivetable":
                        df.write.mode(dest["mode"].any()).saveAsTable(dest["table"].any())
                    elif dest['fileType'].any() == "jdbcclient":
                        df.write.format("jdbc").mode(dest["mode"].any()).option("numPartitions", 8).option("url", dest["url"].any()).option("driver", dest["driver"].any()).option("dbtable", dest["table"].any()).option("user", dest["user"].any()).option("password",dest["password"].any()).save()
                except Exception as e:
                    print (str(datetime.datetime.now()) + "____________ Exception occurred in processData() ________________")
                    print(str(datetime.datetime.now()) + " The iteration key for target Map is :: "+destKey)
                    raise Exception("Exception::msg %s" %str(e))
    #spark.stop()


def processFiles(argTuple):
    #spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    prc = pd.read_json(argTuple[0])
    for prcIdx, prcRow in prc[prc['isActive']=="True"].iterrows():
        prepareMeta(argTuple[1],prcRow)












def main(configPath,args):
    # parse existing file
    config.read(configPath)
    #Read Process files and set thread pool
    spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    mprc = ThreadPool(3)
    lst=glob.glob(config.get('DIT_setup_config', 'prcDetails')+'prc_PrcId_[0-9].json')
    #sprkSession=spark.newSession()
    mprc.map(processFiles,zip(lst, repeat(spark.newSession())))
    #spark.stop()   


if __name__ == "__main__" :
    sys.exit(main('C:\\Users\\sk250102\\Documents\\Teradata\\DIT\\DataIngestionTool\\config\\config.cnf',sys.argv))
    #sys.exit(main('C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\config\\config.cnf', sys.argv))