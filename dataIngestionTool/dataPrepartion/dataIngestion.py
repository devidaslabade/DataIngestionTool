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

#sys.path.append('../')
import dataIngestionTool.comnUtil.logr as logg

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

# instantiate
config = ConfigParser()

def findMapping(uniqSrc,uniqDest):
    # spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    try:
        if uniqSrc == 1 and uniqDest == 1:
            return "One_to_One"
        elif uniqSrc > 1 and uniqDest == 1:
            return "Many_to_One"
        elif uniqSrc == 1 and uniqDest > 1:
            return "One_to_Many"
        elif uniqSrc > 1 and uniqDest > 1:
            return "Many_to_Many"
    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in findMapping() ________________")
        print(str(datetime.datetime.now()) + " The exception occurred for :: " + uniqSrc+" :: "+uniqDest)
        print("Exception::msg %s" % str(e))
        print(traceback.format_exc())
        
def singleSrcPrc(spark,srcMap, schemaMap, destMap, queryMap, spark_logger):
    for srcKey, src in srcMap.items():
        spark_logger.warn("The processing singleSrcPrc() process for " + srcKey)
        try:
            if src['fileType'].any() == "csv" or src['fileType'].any() == "json" or src[
                'fileType'].any() == "parquet" or src['fileType'].any() == "orc":
                df = spark.read.schema(schemaMap[srcKey]).option("header", src['header'].any()).csv(
                    src['srcLocation'].any())
                df.show()
                df.printSchema()
            elif src['fileType'].any() == "hivetable":
                colName = ','.join(schemaMap[srcKey].fieldNames())
                df = spark.sql('SELECT ' + colName + ' FROM ' + src["table"].any())
                print("read from table" + src["table"].any())
                df.show()
            elif src['fileType'].any() == "jdbcclient":
                print(src["table"].any())
                df = spark.read.format("jdbc").option("url", src["url"].any()).option("driver",
                                                                                      src["driver"].any()).option(
                    "dbtable", src["table"].any()).option("user", src["user"].any()).option("password", src[
                    "password"].any()).load()
                df.show()
        except Exception as e:
            print(str(datetime.datetime.now()) + "____________ Exception occurred in processData() ________________")
            print(str(datetime.datetime.now()) + " The iteration key for srcMap is :: " + srcKey)
            print("Exception::msg %s" % str(e))
            print(traceback.format_exc())
            
        for destKey, dest in destMap.items():
            print(queryMap[destKey])
            try:
                if dest['fileType'].any() == "csv" or dest['fileType'].any() == "json" or dest[
                    'fileType'].any() == "orc" or dest['fileType'].any() == "parquet":
                    df.selectExpr(queryMap[destKey]).write.mode(dest["mode"].any()).format(dest["fileType"].any()).save(
                        dest["destLocation"].any() + dest["destId"].any() + "_" + dest["fileType"].any() + "/" + dest[
                            "fileType"].any())
                    df.selectExpr(queryMap[destKey]).show(truncate=False)
                elif dest['fileType'].any() == "hivetable":
                    df.write.mode(dest["mode"].any()).saveAsTable(dest["table"].any())
                elif dest['fileType'].any() == "jdbcclient":
                    df.write.format("jdbc").mode(dest["mode"].any()).option("numPartitions", 8)\
                        .option("url", dest["url"].any()).option("driver", dest["driver"].any())\
                        .option("dbtable",dest["table"].any()).option("user",dest["user"].any())\
                        .option("password", dest["password"].any()).save()
                print("Data inserted successfully for---------- " + destKey )
            except Exception as e:
                print(
                    str(datetime.datetime.now()) + "____________ Exception occurred in processData() ________________")
                print(str(datetime.datetime.now()) + " The iteration key for target Map is :: " + destKey)
                print("Exception::msg %s" % str(e))
                print(traceback.format_exc())
    



        
def prepareMeta(sprkSession, prcRow):
    spark_logger = logg.Log4j(sprkSession, prcRow['prcId'])
    spark_logger.warn("_________________Started processing process Id : " + prcRow['prcId'] + " : ____________________")
    try:
        queryMap = {}
        schemaMap = {}
        srcMap = {}
        destMap = {}
        # Fetch process Id specific mapping file
        maps = pd.read_json(config.get('DIT_setup_config', 'prcMapping') + 'colMapping_' + prcRow['mapId'] + '.json')
        mapTab = maps[maps['mapId'] == prcRow['mapId']]
        for mapId, mapRow in mapTab.iterrows():
            # Fetch source and destination column mapping files
            srcColMap = pd.read_json(config.get('DIT_setup_config', 'srcCols') + 'srcCols_' + mapRow['srcId'] + '.json')
            destColMap = pd.read_json(config.get('DIT_setup_config', 'destCols') + 'destCols_' + mapRow['destId'] + '.json')
            srcCol = srcColMap[(srcColMap['srcId'] == mapRow['srcId']) & (srcColMap['colId'] == mapRow['srcColId'])]
            destCol = destColMap[(destColMap['destId'] == mapRow['destId']) & (destColMap['colId'] == mapRow['destColId'])]
            # query.append(srcCol['colName'].str.cat()+" as "+destCol['colName'].str.cat())
            srcDest = mapRow['srcId'] + ":" + mapRow['destId']
            query= []
            if srcCol.empty :
                query.append("cast(" + destCol['default'].astype(str).str.cat() + " as " + destCol['colType'].str.cat() + " ) as " + destCol['colName'].str.cat())
                print("in block 1"+destCol['colType'].str.cat() )
            elif destCol.get('transFunc') is None or destCol.get('transFunc').empty or destCol.get('transFunc').isnull().any().any() or destCol.get('transFunc').item()== "NA":
                query.append("cast(" + srcCol['colName'].str.cat() + " as " + destCol['colType'].str.cat() + " ) as " + destCol['colName'].str.cat())
                print("in block 2"+srcCol['colName'].str.cat())
            else :
                query.append("cast(" + destCol['transFunc'].str.cat().format(srcCol['colName'].str.cat())+  " as " + destCol['colType'].str.cat() + " ) as " + destCol['colName'].str.cat())
                print("in block 3"+srcCol['colName'].str.cat())
            
            if srcDest not in queryMap :
                queryMap[srcDest] = query
            else :
                tmpQuery = queryMap[srcDest]
                tmpQuery.extend(query)
                queryMap[srcDest] = tmpQuery

            ## Fetch schema of the sources
            if srcDest not in schemaMap:
                fields = fetchSchema(srcColMap[srcColMap['srcId'] == mapRow['srcId']], spark_logger)
                schema = StructType(fields)
                schemaMap[srcDest] = schema
                # Fetch source and destination details
                src = pd.read_json(config.get('DIT_setup_config', 'srcDetails') + 'src_' + mapRow['srcId'] + '.json')
                dest = pd.read_json(config.get('DIT_setup_config', 'destDetails') + 'dest_' + mapRow['destId'] + '.json')
                srcMap[srcDest] = src[src['srcId'] == mapRow['srcId']]
                destMap[srcDest] = dest[dest['destId'] == mapRow['destId']]
        print("Above process data --------",queryMap)
        mapping=findMapping(mapTab.srcId.nunique(),mapTab.destId.nunique())
        print("mapping----------" +mapping)
        processData(sprkSession,mapping, srcMap, schemaMap, destMap, queryMap, spark_logger)
    except Exception as e:
        spark_logger.warn(str(datetime.datetime.now()) + "____________ Exception occurred in prepareMeta() ________________")
        spark_logger.warn(str(datetime.datetime.now()) + " The exception occurred for process ID :: " + prcRow['prcId'])
        spark_logger.warn("Exception::msg %s" % str(e)) 
        print(traceback.format_exc())       

                
def fetchSchema(srcCols, spark_logger):
    try:
        spark_logger.warn("Fetching schema values for SRC Id " + srcCols['srcId'].str.cat())
        fields = []
        for idx, clm in srcCols.iterrows():
            if clm['colType'].lower() == "String".lower():
                colField = StructField(clm['colName'], StringType(), eval(clm['isNullable']))
                fields.append(colField)
            elif clm['colType'].lower() == "Int".lower():
                colField = StructField(clm['colName'], IntegerType(), eval(clm['isNullable']))
                fields.append(colField)
            elif clm['colType'].lower() == "Long".lower():
                colField = StructField(clm['colName'], LongType(), eval(clm['isNullable']))
                fields.append(colField)
            elif clm['colType'].lower() == "Float".lower():
                colField = StructField(clm['colName'], FloatType(), eval(clm['isNullable']))
                fields.append(colField)
            elif clm['colType'].lower() == "Double".lower():
                colField = StructField(clm['colName'], DoubleType(), eval(clm['isNullable']))
                fields.append(colField)
            elif clm['colType'].lower() == "Boolean".lower():
                colField = StructField(clm['colName'], BooleanType(), eval(clm['isNullable']))
                fields.append(colField)
            elif clm['colType'].lower() == "Timestamp".lower():
                colField = StructField(clm['colName'], TimestampType(), eval(clm['isNullable']))
                fields.append(colField)
            else:
                colField = StructField(clm['colName'], StringType(), eval(clm['isNullable']))
                fields.append(colField)
        return fields
    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in fetchSchema() ________________")
        print(str(datetime.datetime.now()) + " The exception occurred for Src Id :: " + srcCols['srcId'].str.cat())
        print("Exception::msg %s" % str(e)) 
        print(traceback.format_exc())

def processData(spark,mapping, srcMap, schemaMap, trgtMap, queryMap, spark_logger):
    # TODO find alternative to any and restrict it to one row using tail head etc
    # #If source and destination has one entries both side
    print("src Map")
    print(srcMap)
    print("schemaMap")
    print(schemaMap)
    print("trgtMap")
    print(trgtMap)
    print("queryMap")
    print(queryMap)
    if mapping== "One_to_One" or mapping== "One_to_Many":
        print("queryMap")
        print(queryMap)
        singleSrcPrc(spark,srcMap, schemaMap, trgtMap, queryMap, spark_logger)
    elif mapping == "Many_to_One"  :
        print("in "+mapping) 
    elif mapping == "Many_to_Many" :
        print("in "+mapping)
   
    
                # raise Exception("Exception::msg %s" % str(e))
    # spark.stop()


def processFiles(argTuple):
    # spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    try:
        prc = pd.read_json(argTuple[0])
        for prcIdx, prcRow in prc[prc['isActive'] == "True"].iterrows():
            prepareMeta(argTuple[1], prcRow)
    except Exception as e:
            print(str(datetime.datetime.now()) + "____________ Exception occurred in processFiles() ________________")
            print(str(datetime.datetime.now()) + " The exception occured for :: " + argTuple[0])
            print("Exception::msg %s" % str(e))        
            print(traceback.format_exc())

def main(configPath, prcPattern,pool):
    # parse existing file
    config.read(configPath)
    # Read Process files and set thread pool
    prcList = list()
    #regex = "r\'"+prcPattern+"\'".encode('string_escape')    
    #regex = r'(colMapping_cm_(1|21|12)|prc_(PrcId_[0-9])).json'
    #print(regex)
    for dir, root, files in os.walk(config.get('DIT_setup_config', 'prcDetails')):
        matches = re.finditer(r'{0}'.format(prcPattern), ' '.join(files), re.MULTILINE)
        #matches = re.finditer(regex, ' '.join(files), re.MULTILINE)
        for matchNum, match in enumerate(matches):
            prcList.append(os.path.join(dir, match.group()))
    
    threadPool = ThreadPool(pool)
    print("list of files are ::", prcList)
    spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    
    # lst = glob.glob(config.get('DIT_setup_config', 'prcDetails') + 'prc_PrcId_[0-9].json')
    # sprkSession=spark.newSession()
    threadPool.map(processFiles, zip(prcList, repeat(spark.newSession())))
    # spark.stop()


if __name__ == "__main__":
    prcs="prc_PrcId_[0-9].json"
    pool=3
    #sys.exit(main('C:\\Users\\sk250102\\Documents\\Teradata\\DIT\\DataIngestionTool\\config\\config.cnf', prcs,pool))
    sys.exit(main('C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\config\\config.cnf',prcs,pool))

