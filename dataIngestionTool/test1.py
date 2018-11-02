import os
import re
import sys
import json
import glob
from multiprocessing.dummy import Pool as ThreadPool
from itertools import repeat
import pandas as pd
import datetime
sys.path.append('../')
import comnUtil.logr as logg
import traceback

try:
    import pyspark
except:
    import findspark

    findspark.init()
    import pyspark
# from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import *
# from pyspark.sql.types import StructType, StructField
# from pyspark.sql.types import DoubleType, IntegerType, StringType
from configparser import ConfigParser

# instantiate
config = ConfigParser()


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
        print(mapTab)
        for mapId, mapRow in mapTab.iterrows():
            # Fetch source and destination column mapping files
            srcColMap = pd.read_json(config.get('DIT_setup_config', 'srcCols') + 'srcCols_' + mapRow['srcId'] + '.json')
            destColMap = pd.read_json(
            config.get('DIT_setup_config', 'destCols') + 'destCols_' + mapRow['destId'] + '.json')
            srcCol = srcColMap[(srcColMap['srcId'] == mapRow['srcId']) & (srcColMap['colId'] == mapRow['srcColId'])]
            destCol = destColMap[
                (destColMap['destId'] == mapRow['destId']) & (destColMap['colId'] == mapRow['destColId'])]
            # query.append(srcCol['colName'].str.cat()+" as "+destCol['colName'].str.cat())

            src = mapRow['srcId']
            dest = mapRow['destId']
            srcDest = src + ":" + dest

            #query="cast(" + srcCol['colName'].str.cat() + " as " + destCol['colType'].str.cat() + " ) as " + destCol[
             #       'colName'].str.cat()
            query= []
            query.append(
                "cast(" + srcCol['colName'].str.cat() + " as " + destCol['colType'].str.cat() + " ) as " +
                destCol[
                    'colName'].str.cat())
            if srcDest not in queryMap :
                print("Inside srcDest",query)
                #emptLst=[]
                #emptLst.append(query)
                queryMap[srcDest] = query
                print("Inside if of srcDest-------- ", queryMap)
            else :
                tmp = queryMap[srcDest]
                print(type(tmp))
                print("the val of tmp is ", tmp)
                queryMap[srcDest] = queryMap[srcDest].__add__(query)
                print("Inside else of srcDest--------", queryMap)


            # #Fetch schema of the sources

            if srcDest not in schemaMap:
                fields = fetchSchema(srcColMap[srcColMap['srcId'] == mapRow['srcId']], spark_logger)
                schema = StructType(fields)
                schemaMap[mapRow['srcId']] = schema
                # Fetch source and destination details
                src = pd.read_json(config.get('DIT_setup_config', 'srcDetails') + 'src_' + mapRow['srcId'] + '.json')
                dest = pd.read_json(
                    config.get('DIT_setup_config', 'destDetails') + 'dest_' + mapRow['destId'] + '.json')
                srcMap[mapRow['srcId']] = src[src['srcId'] == mapRow['srcId']]
                destMap[mapRow['destId']] = dest[dest['destId'] == mapRow['destId']]
        #print(schemaMap)
        #print(srcMap)
        #print(destMap)
        print("Above process data --------",queryMap)
            #for ls in kval:
            #    print("val is" + ls)
        #processData(sprkSession, srcMap, schemaMap, destMap, query, queryMap,spark_logger)
        mapping = findMapping(queryMap)
        print("mapping----------" +mapping)
    except Exception as e:
        spark_logger.warn(
            str(datetime.datetime.now()) + "____________ Exception occurred in prepareMeta() ________________")
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
            elif clm['colType'].lower() == "Float".lower():
                colField = StructField(clm['colName'], FloatType(), eval(clm['isNullable']))
                fields.append(colField)
            elif clm['colType'].lower() == "Double".lower():
                colField = StructField(clm['colName'], DoubleType(), eval(clm['isNullable']))
                fields.append(colField)
            elif clm['colType'].lower() == "Boolean".lower():
                colField = StructField(clm['colName'], BooleanType(), eval(clm['isNullable']))
                fields.append(colField)
            else:
                colField = StructField(clm['colName'], StringType(), eval(clm['isNullable']))
                fields.append(colField)
        return fields
    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in fetchSchema() ________________")
        print(str(datetime.datetime.now()) + " The exception occurred for Src Id :: " + srcCols['srcId'].str.cat())
        print("Exception::msg %s" % str(e))


def processData(spark, srcMap, schemaMap, trgtMap, query, queryMap,spark_logger):
    # TODO find alternative to any and restrict it to one row using tail head etc
    # #If source and destination has one entries both side
    for srcKey, src in srcMap.items():
        try:
            if schemaMap.__len__() > 1 :
                print("In length 2")
                if src['fileType'].any() == "csv" or src['fileType'].any() == "json" or src[
                    'fileType'].any() == "parquet" or src['fileType'].any() == "orc":
                    print("start schema map length")

                    if srcKey == "SrcId_8":
                        df1 = spark.read.schema(schemaMap[srcKey]).option("header", src['header'].any()).csv(
                            src['srcLocation'].any())
                    else:
                        df2 = spark.read.schema(schemaMap[srcKey]).option("header", src['header'].any()).csv(
                            src['srcLocation'].any())
                    df1.show()
                    df2.show()
                    ta = df1.alias('ta')
                    tb = df2.alias('tb')
                    df = ta.join(tb, ta.category_department_id == tb.department_id).select(ta.category_name,tb.department_name)
                    df.show()
                    print("end schema map length")
            elif schemaMap.__len__() == 1 :
                print("In length 1")
                table = "temp_table"
                if src['fileType'].any() == "csv" or src['fileType'].any() == "json" or src[
                   'fileType'].any() == "parquet" or src['fileType'].any() == "orc":
                    print("in if of files")
                    df = spark.read.schema(schemaMap[srcKey]).option("header", src['header'].any()).csv(
                        src['srcLocation'].any())
                    df.registerTempTable(table)
                    df.show()
                    df.printSchema()
                elif src['fileType'].any() == "hivetable":
                    print("in if of hivetable")
                    print(schemaMap[srcKey].fieldNames())
                    colName = ','.join(schemaMap[srcKey].fieldNames())
                    print(colName)
                    #read from file
                    #df = spark.read.schema(schemaMap[srcKey]).option("header", src['header'].any()).csv(
                    #    src['srcLocation'].any())

                    #Read from table
                    df = spark.sql('SELECT ' + colName + ' FROM ' + src["table"].any())
                    df.show()
                elif src['fileType'].any() == "jdbcclient":
                    print("in if of jdbcclient")
                    print(src["table"].any())
                    df = spark.read.format("jdbc").option("url", src["url"].any()).option("driver",src["driver"].any()).option(
                        "dbtable", src["table"].any()).option("user", src["user"].any()).option("password", src[
                        "password"].any()).load()
                    df.show()
        except Exception as e:
            print(str(datetime.datetime.now()) + "____________ Exception occurred in processData() ________________")
            print(str(datetime.datetime.now()) + " The iteration key for srcMap is :: " + srcKey)
            print("Exception::msg %s" % str(e))
            print(traceback.format_exc())
    print(trgtMap.__len__())
    #destination has one entries  side
    for destKey, dest in trgtMap.items():
        print(query)
        try:
            if dest['fileType'].any() == "csv" or dest['fileType'].any() == "json" or dest[
                'fileType'].any() == "orc" or dest['fileType'].any() == "parquet":
                print("start dest ")

                if queryMap.__len__() > 1:

                    for k,v in queryMap.items():
                        print("key is - ----" + k)
                        tokens = k.split(":")
                        print("token is - ----" + tokens[1])

                        if tokens[1] == destKey :
                            colName = ','.join(v)
                            print(colName)
                            df = spark.sql('select ' + colName + ' from temp_table')
                            df.show()
                            df.write.mode(dest["mode"].any()).format(dest["fileType"].any()).save(
                                dest["destLocation"].any() + dest["destId"].any() + "_" + dest["fileType"].any() + "/" +
                                dest[
                                    "fileType"].any())
                else :
                    print("Query Map contains 1 va")
                    df.write.mode(dest["mode"].any()).format(dest["fileType"].any()).save(
                        dest["destLocation"].any() + dest["destId"].any() + "_" + dest["fileType"].any() + "/" + dest[
                            "fileType"].any())

                print("end dest ")
            elif dest['fileType'].any() == "hivetable":
                df.write.mode(dest["mode"].any()).saveAsTable(dest["table"].any())
            elif dest['fileType'].any() == "jdbcclient":
                df.write.format("jdbc").mode(dest["mode"].any()).option("numPartitions", 8).option("url", dest[
                    "url"].any()).option("driver", dest["driver"].any()).\
                    option("dbtable",dest["table"].any()).option("user",dest["user"].any()).option( "password", dest["password"].any()).save()
        except Exception as e:
             print(str(datetime.datetime.now()) + "____________ Exception occurred in processData() ________________")
             print(str(datetime.datetime.now()) + " The iteration key for target Map is :: " + destKey)
             print("Exception::msg %s" % str(e))
             print(traceback.format_exc())
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

def main(configPath, prcPattern, pool):
    # parse existing file
    config.read(configPath)
    # Read Process files and set thread pool
    prcList = list()
    # regex = r'prc_PrcId_[0-9].json'
    # regex = r'(colMapping_cm_(1|21|12)|prc_(PrcId_[0-9])).json'
    # print(regex)
    for dir, root, files in os.walk(config.get('DIT_setup_config', 'prcDetails')):
        matches = re.finditer(r'{0}'.format(prcPattern), ' '.join(files), re.MULTILINE)
        for matchNum, match in enumerate(matches):
            prcList.append(os.path.join(dir, match.group()))

    threadPool = ThreadPool(pool)
    print("list of files are ::", prcList)
    spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()

    # lst = glob.glob(config.get('DIT_setup_config', 'prcDetails') + 'prc_PrcId_[0-9].json')
    # sprkSession=spark.newSession()
    threadPool.map(processFiles, zip(prcList, repeat(spark.newSession())))
    # spark.stop()

def findMapping(queryMap):
    # spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    try:
        srcTokens = list()
        destTokens = list()
        print(queryMap)
        for k,v in queryMap.items() :
            print("key is - ----" +k)
            print("value is - ----", v)

            tokens = k.split(":")
            #print("tokens is - ----" + tokens)
            fields = ','.join(set(tokens))
            print("fields is - ----" + fields)
            count = 0
            for s in tokens:
                print("String is " + s)
                if re.match(r'^Src', s):
                    print("Inside src tockens")
                    srcTokens.append(s)
                elif re.match(r'^Dest', s):
                    print("Inside dest tockens")
                    destTokens.append(s)
                    print("End")

        uniqueSrc = ",".join(set(srcTokens))
        uniqueDest = ",".join(set(destTokens))
        uniqueSrcList = list(uniqueSrc.split(","))
        uniqueDestList = list(uniqueDest.split(","))

        print("uniqueSrcList is - ----", uniqueSrcList.__len__())
        print("uniqueDestList is - ----", uniqueDestList.__len__())
        print("srcTokens is - ----", srcTokens)
        print("destTokens is - ----", destTokens)

        if uniqueSrcList.__len__() == 1 and uniqueDestList.__len__() == 1:
            return "One_to_One"
        elif uniqueSrcList.__len__() > 1 and uniqueDestList.__len__() == 1:
            return "Many_to_One"
        elif uniqueSrcList.__len__() == 1 and uniqueDestList.__len__() > 1:
            return "One_to_Many"
        elif uniqueSrcList.__len__() > 1 and uniqueDestList.__len__() > 1:
            return "Many_to_Many"







    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in processFiles() ________________")
        print(str(datetime.datetime.now()) + " The exception occured for :: " + queryMap[0])
        print("Exception::msg %s" % str(e))
        print(traceback.format_exc())


if __name__ == "__main__":
    prcs = "prc_PrcId_[0-9].json"
    pool = 3
    #sys.exit(main('C:\\Users\\sk250102\\Documents\\Teradata\\DIT\\DataIngestionTool\\config\\config.cnf', prcs, pool))
    sys.exit(main('C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\config\\config.cnf', prcs, pool))

