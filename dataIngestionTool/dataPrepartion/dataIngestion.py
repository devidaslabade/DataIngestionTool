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
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from kafka import KafkaConsumer
import logr as logg
from babel.util import distinct

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


def prepareTPTScript(spark,srcMap, schemaMap, destMap, queryMap, spark_logger):
    for srcKey, src in srcMap.items():
        spark_logger.warn("The processing singleSrcPrc() process for " + srcKey.split(":")[0])
        #spark_logger.warn("_________________Started processing process Id : " + prcRow['prcId'] + " : ____________________")
        try:
            print("TEST200:", )
            tptFolder = config.get('DIT_setup_config', 'tptFolder')
            print("src Map")
            print(srcMap)
            print("schemaMap")
            print(schemaMap)
            print("destMap")
            print(destMap)
            print("queryMap")
            print(queryMap)
            srcColMap = pd.read_json(config.get('DIT_setup_config', 'srcCols') + 'srcCols_' + srcKey.split(":")[0] + '.json')
            print(srcColMap)
            destColMap = pd.read_json(config.get('DIT_setup_config', 'destCols') + 'destCols_' + srcKey.split(":")[1] + '.json')
            print(destColMap)
            #fname = tptFolder + PrcName + ".tpt"
            #print("TEST201:" + fname)
            #f_tpt = open(fname, "w")
    
            #f_tpt.write(ProcName)
            #f_tpt.write("\n")
    
    
        #f_tpt.close()

        except Exception as e:
            spark_logger.warn(str(datetime.datetime.now()) + "____________ Exception occurred in prepareTPTScript() ________________")
            spark_logger.warn(str(datetime.datetime.now()) + " The exception occurred for process ID :: " + srcKey)
            spark_logger.warn("Exception::msg %s" % str(e))  
            print(traceback.format_exc())


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
        
        
def prepareJoinCodition(joinCondition,srcDest,prcRow,srcColMap):
    try:
        for row in prcRow['joinCol'].split("=") :
            if srcDest.split(":")[0] in row.split(":")[0] :
                srcCol = srcColMap[(srcColMap['srcId'] == srcDest.split(":")[0]) & (srcColMap['colId'] == int(row.split(":")[1]))]
                print(srcDest.split(":")[0]+"."+srcCol['colName'].str.cat())
                if "=" not in joinCondition :
                    joinCondition += srcDest.split(":")[0]+" inner join {tab} on "+srcDest.split(":")[0]+"."+srcCol['colName'].str.cat()+" = {col}"
                    #joinCondition.format(tab1=srcDest.split(":")[0],col1= srcDest.split(":")[0]+"."+srcCol['colName'].str.cat()+"=")
                else :
                    #joinCondition += srcDest.split(":")[0]+"."+srcCol['colName'].str.cat()
                    joinCondition=joinCondition.format(tab = srcDest.split(":")[0], col = srcDest.split(":")[0]+"."+srcCol['colName'].str.cat())
        return joinCondition            
    except Exception as e:
                print(
                    str(datetime.datetime.now()) + "____________ Exception occurred in prepareJoinCodition() ________________")
                print(str(datetime.datetime.now()) + " The iteration key  is :: " + srcDest)
                print("Exception::msg %s" % str(e))
                print(traceback.format_exc())            


def prepareFilterCodition(srcDest,prcRow,srcColMap):
    try:
        print("Processing filter condition for "+srcDest)
        for row in prcRow['filterCondition'].split("@") :
            if srcDest.split(":")[0] in row.split(":")[0] :
                #Fetch the column details from src col mapping having same srcID and ColID
                srcCol = srcColMap[(srcColMap['srcId'] == srcDest.split(":")[0]) & (srcColMap['colId'] == int(row.split(":")[1]))]
                return " Where "+srcDest.split(":")[0]+"."+srcCol['colName'].str.cat()+ prcRow['filterCondition'].split("@")[1]
            else :
                return ""            
    except Exception as e:
                print(
                    str(datetime.datetime.now()) + "____________ Exception occurred in prepareFilterCodition() ________________")
                print(str(datetime.datetime.now()) + " The iteration key  is :: " + srcDest)
                print("Exception::msg %s" % str(e))
                print(traceback.format_exc()) 


                        
def singleSrcPrc(spark,srcMap, schemaMap, destMap, queryMap,filterCondition, spark_logger):
    for srcKey, src in srcMap.items():
        spark_logger.warn("The processing singleSrcPrc() process for " + srcKey)
        try:
            if  src['fileType'].any() == "json" or src['fileType'].any() == "parquet" or src['fileType'].any() == "orc":
                df = spark.read.format(src['fileType'].any()).schema(schemaMap[srcKey]).load(src['srcLocation'].any())
                #.option("inferSchema", src.get('inferSchema').str.cat().lower()) Not required
            elif src['fileType'].any() == "csv" or src['fileType'].any() == "delimited":
                if src.get('delimiter') is None :
                    delimiter=","
                else :
                    delimiter=src.get('delimiter').str.cat()    
                if src.get('inferSchema') is None or src.get('inferSchema').str.cat().lower() == "false" :
                    df = spark.read.format("csv").schema(schemaMap[srcKey]).option("header", src['header'].any()).option("delimiter", delimiter).load(src['srcLocation'].any())
             
                else:
                    df = spark.read.format("csv").option("header", src['header'].any()).option("delimiter", delimiter).option("inferSchema", src.get('inferSchema').str.cat().lower()).load(src['srcLocation'].any())

            elif src['fileType'].any() == "hivetable":
                print("Inside hivetable")
                colName = ','.join(schemaMap[srcKey].fieldNames())
                df = spark.sql('SELECT ' + colName + ' FROM ' + src["table"].any())
                print("read from table" + src["table"].any())
            elif src['fileType'].any() == "jdbcclient":
                print("Inside jdbcclient")
                print(src["table"].any())
                df = spark.read.format("jdbc").option("url", src["url"].any()).option("driver",src["driver"].any()).option("dbtable", src["table"].any()).option("user", src["user"].any()).option("password", src["password"].any()).load()
            df.show()
            df.printSchema()  
            df.createOrReplaceTempView(srcKey.split(":")[0])
        except Exception as e:
            print(str(datetime.datetime.now()) + "____________ Exception occurred in processData() ________________")
            print(str(datetime.datetime.now()) + " The iteration key for srcMap is :: " + srcKey)
            print("Exception::msg %s" % str(e))
            print(traceback.format_exc())

            
        for destKey, dest in destMap.items():
            print(queryMap[destKey])
            print(','.join(queryMap[destKey]))
            try:
                #Fetch value of compression
                if dest.get('compression') is None :
                    compression="none"
                else :
                    compression=dest.get('compression').str.cat() 
                    
                    
                if dest['fileType'].any() == "csv" or dest['fileType'].any() == "json" or dest[
                    'fileType'].any() == "orc" or dest['fileType'].any() == "parquet":
                    spark.sql("select "+','.join(queryMap[destKey])+" from "+destKey.split(":")[0]+filterCondition).write.mode(dest["mode"].any()).format(dest["fileType"].any()).option("compression",compression).save(
                        dest["destLocation"].any() + dest["destId"].any() + "_" + dest["fileType"].any() + "/" + dest[
                            "fileType"].any())
                    spark.sql("select "+','.join(queryMap[destKey])+" from "+destKey.split(":")[0]+filterCondition).show(truncate=False)
                    #df.selectExpr(queryMap[destKey]).show(truncate=False)
                elif dest['fileType'].any() == "hivetable":
                    df.write.mode(dest["mode"].any()).saveAsTable(dest["table"].any())
                elif dest['fileType'].any() == "jdbcclient":
                    df.write.format("jdbc").mode(dest["mode"].any()).option("numPartitions", 8)\
                        .option("url", dest["url"].any()).option("driver", dest["driver"].any())\
                        .option("dbtable",dest["table"].any()).option("user",dest["user"].any())\
                        .option("password", dest["password"].any()).save()
                    print("Data inserted successfully for---------- " + destKey )
                elif dest['fileType'].any() == "DataBase":
                    print("TEST107c::")
                    prepareTPTScript(spark,srcMap, schemaMap, destMap, queryMap, spark_logger)


                #print("------------Start of Destination Statistics----------")
                #destStat = df.selectExpr(queryMap[destKey]).describe()
                #destStat.show()
                #destStat.createOrReplaceTempView("dest")
                #print("--------------End of Destination Statistics----------")

                #print("------------Start of Comaparison Statistics----------")
                #spark.sql("select s.summary,s.category_id, d.cat_id,(s.category_id - d.cat_id) AS Category_diff,s.category_department_id,d.cat_dpt_id,(s.category_department_id - d.cat_dpt_id) AS diff_Category_department,s.category_name,d.cat_name,(s.category_name = d.cat_name) AS diff_Category_Name from source s join dest d on s.summary  = d.summary").show()
                #spark.sql("select CAST(s.category_name AS int),CAST(d.cat_name AS INT) from source s join dest d on s.summary  = d.summary").show()
                #print("------------End of Comaparison Statistics----------")


            except Exception as e:
                print(str(datetime.datetime.now()) + "____________ Exception occurred in processData() ________________")
                print(str(datetime.datetime.now()) + " The iteration key for target Map is :: " + destKey)
                print("Exception::msg %s" % str(e))
                print(traceback.format_exc())
    
def multiSrcPrc(spark,srcMap, schemaMap, destMap, queryMap,joinCondition,filterCondition, spark_logger):
    for srcKey, src in srcMap.items():
        spark_logger.warn("The processing singleSrcPrc() process for " + srcKey)
        try:
            if src['fileType'].any() == "csv" or src['fileType'].any() == "json" or src[
                'fileType'].any() == "parquet" or src['fileType'].any() == "orc":
                df = spark.read.schema(schemaMap[srcKey]).option("header", src['header'].any()).csv(
                    src['srcLocation'].any())
                df.createOrReplaceTempView(srcKey.split(":")[0])
                df.show()
                df.printSchema()
            elif src['fileType'].any() == "hivetable":
                colName = ','.join(schemaMap[srcKey].fieldNames())
                df = spark.sql('SELECT ' + colName + ' FROM ' + src["table"].any())
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
            
    query="select "
    distinctDest="NA"        
    for key, querylst in queryMap.items():
        print("Adding query for"+key)
        query+=','.join(querylst)+","
        
                
            
    for destKey, dest in destMap.items():
        print("in for loop"+destKey)
        print(distinctDest not in destKey.split(":")[1])
        print(distinctDest)
        if distinctDest not in destKey.split(":")[1]:
            distinctDest=destKey.split(":")[1]
            print("in if loop")
            try:
                if dest['fileType'].any() == "csv" or dest['fileType'].any() == "json" or dest['fileType'].any() == "orc" or dest['fileType'].any() == "parquet":
                    #df.selectExpr(queryMap[destKey]).write.mode(dest["mode"].any()).format(dest["fileType"].any()).save(dest["destLocation"].any() + dest["destId"].any() + "_" + dest["fileType"].any() + "/" + dest[
                    #            "fileType"].any())
                    #df.selectExpr(queryMap[destKey]).show(truncate=False)
                    print(":::::Executing Query::::::",query[0:-1]+joinCondition+filterCondition)
                    spark.sql(query[0:-1]+joinCondition+filterCondition).show(truncate=False)
                elif dest['fileType'].any() == "hivetable":
                    df.write.mode(dest["mode"].any()).saveAsTable(dest["table"].any())
                elif dest['fileType'].any() == "jdbcclient":
                    df.write.format("jdbc").mode(dest["mode"].any()).option("numPartitions", 8)\
                        .option("url", dest["url"].any()).option("driver", dest["driver"].any())\
                        .option("dbtable",dest["table"].any()).option("user",dest["user"].any())\
                        .option("password", dest["password"].any()).save()
                elif dest['fileType'].any() == "DataBase":
                    print("TEST107c::")
                    prepareTPTScript(spark,srcMap, schemaMap, destMap, queryMap, spark_logger)        
            except Exception as e:
                print(str(datetime.datetime.now()) + "____________ Exception occurred in processData() ________________")
                print(str(datetime.datetime.now()) + " The iteration key for target Map is :: " + destKey)
                print("Exception::msg %s" % str(e))
                print(traceback.format_exc())

        
def prepareMeta(sprkSession, prcRow):
    possibleError=""
    spark_logger = logg.Log4j(sprkSession, prcRow['prcId'])
    spark_logger.warn("_________________Started processing process Id : " + prcRow['prcId'] + " : ____________________")
    try:
        queryMap = {}
        schemaMap = {}
        srcMap = {}
        destMap = {}
        #joinCondition=" from {tab1} inner join {tab2} on {col1} = {col2}"
        joinCondition=" from "
        filterCondition= ""
        # Fetch process Id specific mapping file
        maps = pd.read_json(config.get('DIT_setup_config', 'prcMapping') + 'colMapping_' + prcRow['mapId'] + '.json')
        mapTab = maps[maps['mapId'] == prcRow['mapId']]
        for mapId, mapRow in mapTab.iterrows():
            # Fetch source and destination column mapping files with respect to each source and column 
            srcColMap = pd.read_json(config.get('DIT_setup_config', 'srcCols') + 'srcCols_' + mapRow['srcId'] + '.json')
            destColMap = pd.read_json(config.get('DIT_setup_config', 'destCols') + 'destCols_' + mapRow['destId'] + '.json')
            srcCol = srcColMap[(srcColMap['srcId'] == mapRow['srcId']) & (srcColMap['colId'] == mapRow['srcColId'])]
            destCol = destColMap[(destColMap['destId'] == mapRow['destId']) & (destColMap['colId'] == mapRow['destColId'])]
            # query.append(srcCol['colName'].str.cat()+" as "+destCol['colName'].str.cat())
            srcDest = mapRow['srcId'] + ":" + mapRow['destId']
            query= []
            if srcCol.empty :
                possibleError="\n 1.Default column is not set in Destination \n 2.Process mapping maps to a source column that does not exist"
                query.append("cast(" + destCol['default'].astype(str).str.cat() + " as " + destCol['colType'].str.cat() + " ) as " + destCol['colName'].str.cat())
            elif destCol.get('transFunc') is None or destCol.get('transFunc').empty or destCol.get('transFunc').isnull().any().any() or destCol.get('transFunc').item()== "NA":
                query.append("cast(" +mapRow['srcId'] +"." + srcCol['colName'].str.cat() + " as " + destCol['colType'].str.cat() + " ) as " + destCol['colName'].str.cat())
            else :
                query.append("cast(" + destCol['transFunc'].str.cat().format(mapRow['srcId'] +"." +srcCol['colName'].str.cat())+  " as " + destCol['colType'].str.cat() + " ) as " + destCol['colName'].str.cat())
            
            #For every src:key pair create a SQL query map
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
                #Set join condition  
                if prcRow.get('joinCol') is not None:              
                    joinCondition=prepareJoinCodition(joinCondition,srcDest,prcRow,srcColMap)
                #TODO device a logic to seperately write filter queries 
                if prcRow.get('filterCondition') is not None:     
                    filterCondition+=prepareFilterCodition(srcDest, prcRow, srcColMap)
        #Identify the process mapping     
        mapping=findMapping(mapTab.srcId.nunique(),mapTab.destId.nunique())
        #Process data 
        processData(sprkSession,mapping, srcMap, schemaMap, destMap, queryMap,joinCondition,filterCondition, spark_logger)
    except Exception as e:
        spark_logger.warn(str(datetime.datetime.now()) + "____________ Exception occurred in prepareMeta() ________________")
        spark_logger.warn(str(datetime.datetime.now()) + " The exception occurred for process ID :: " + prcRow['prcId'])
        spark_logger.warn(str(datetime.datetime.now()) + " The possible errors can be "+possibleError)
        spark_logger.warn("Exception::msg %s" % str(e))
        print(traceback.format_exc())
        #publishKafka(sprkSession,prcRow['prcId'],"")

                
def fetchSchema(srcCols, spark_logger):
    try:
        spark_logger.warn("Fetching schema values for SRC Id " + srcCols['srcId'].any())
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

def processData(spark,mapping, srcMap, schemaMap, trgtMap, queryMap,joinCondition,filterCondition, spark_logger):
    # TODO find alternative to any and restrict it to one row using tail head etc
    # #If source and destination has one entries both side
    #print("src Map")
    #print(srcMap)
    #print("schemaMap")
    #print(schemaMap)
    #print("trgtMap")
    #print(trgtMap)
    #print("queryMap")
    #print(queryMap)
    print("The process mapping of the current process is :: " +mapping)
    #print("joining----"+joinCondition)
    #print("the filter condition is "+filterCondition)
    #print("Above process data --------",queryMap)
    msg = "Processing start for active processess"   
    #publishKafka(sprkSession, prcRow['prcId'], msg)
    if mapping== "One_to_One" or mapping== "One_to_Many":
        singleSrcPrc(spark,srcMap, schemaMap, trgtMap, queryMap,filterCondition, spark_logger)
    elif mapping == "Many_to_One"  :
        multiSrcPrc(spark,srcMap, schemaMap, trgtMap, queryMap,joinCondition,filterCondition, spark_logger)
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
    print("List of process files to be processed are :: \n", prcList)
    spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    
    # lst = glob.glob(config.get('DIT_setup_config', 'prcDetails') + 'prc_PrcId_[0-9].json')
    # sprkSession=spark.newSession()
    threadPool.map(processFiles, zip(prcList, repeat(spark.newSession())))
    # spark.stop()

def publishKafka(spark, prcId,msg):
    # spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    try:
        print("Start publishKafka")
        app_id = spark.sparkContext.getConf().get('spark.app.id')
        app_name = spark.sparkContext.getConf().get('spark.app.name')
        current_date = str(datetime.datetime.now())
        jsonString = {"prc_id" : prcId ,"isException": "true","msg":msg}
        producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        #producer.send('foobar', key=b'str(prcId+"-"+app_name+"-"+app_id+"-"+current_date)', value=b'jsonString')
        #producer.send('test', jsonString)
        print("End of publishKafka")

        print("Start consuming")
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
        consumer.subscribe(['test'])
        for msg in consumer:
            print(msg)

        print("End Consuming")
    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in publishKafka() ________________")
        print("Exception::msg %s" % str(e))
        print(traceback.format_exc())



if __name__ == "__main__":
    prcs="prc_PrcId_[0-9].json"
    pool=3
    sys.exit(main('C:\\Users\\sk250102\\Documents\\Teradata\\DIT\\DataIngestionTool\\config\\config.cnf', prcs,pool))
    #sys.exit(main('C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\config\\config.cnf',prcs,pool))

