import os
import re
import sys
import json
import itertools
from multiprocessing.dummy import Pool as ThreadPool
import pandas as pd
import datetime
import traceback
from kafka import KafkaProducer
try:
    import dataPrepartion.custlogger as logg
except:
    import custlogger as logg

try:
    import pyspark
except:
    import findspark
    findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from configparser import ConfigParser

# instantiate config Parser
config = ConfigParser()


def logKey(spark, prcId):
    try:
        current_date = str(datetime.datetime.now().strftime("%Y-%m-%d"))
        app_id = spark.sparkContext.getConf().get('spark.app.id')
        app_name = spark.sparkContext.getConf().get('spark.app.name')
        logkey = str(prcId+"-"+app_name+"-"+app_id+"-"+current_date)
        return logkey
    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in logKey() ________________")
        print("Exception::msg %s" % str(e))
        print(traceback.format_exc())


def publishKafka(producer,spark_logger,prcKey,logLevel,msg):
    try:
        if logLevel == "INFO" or logLevel == "WARN":
            spark_logger.warn(msg)
        else :
            spark_logger.error(msg)     
        jsonString = {"Timestamp":str(datetime.datetime.now()),"LogLevel": logLevel,"LogMsg":msg}
        producer.send(config.get('DIT_Kafka_config', 'TOPIC'), key=prcKey.encode('utf-8'), value=json.dumps(jsonString).encode('utf-8'))
    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in publishKafka() ________________")
        print("Exception::msg %s" % str(e))
        print(traceback.format_exc())

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


def findMapping(uniqSrc,uniqDest,producer,spark_logger):
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
        publishKafka(producer,spark_logger,key,"ERROR","Exception occurred in findMapping()")
        publishKafka(producer,spark_logger,key,"ERROR"," The exception occurred for :: " + uniqSrc+" :: "+uniqDest)
        publishKafka(producer,spark_logger,key,"ERROR","Exception::msg %s" % str(e))
        publishKafka(producer,spark_logger,key,"ERROR",traceback.format_exc())
        
        
def prepareJoinCodition(joinCondition,srcDest,prcRow,srcColMap,key,producer,spark_logger):
    try:
        for row in prcRow['joinCol'].split("=") :
            if srcDest.split(":")[0] in row.split(":")[0] :
                srcCol = srcColMap[(srcColMap['srcId'] == srcDest.split(":")[0]) & (srcColMap['colId'] == int(row.split(":")[1]))]
                #print(srcDest.split(":")[0]+"."+srcCol['colName'].str.cat())
                if "=" not in joinCondition :
                    joinCondition += srcDest.split(":")[0]+" inner join {tab} on "+srcDest.split(":")[0]+"."+srcCol['colName'].str.cat()+" = {col}"
                    #joinCondition.format(tab1=srcDest.split(":")[0],col1= srcDest.split(":")[0]+"."+srcCol['colName'].str.cat()+"=")
                else :
                    #joinCondition += srcDest.split(":")[0]+"."+srcCol['colName'].str.cat()
                    joinCondition=joinCondition.format(tab = srcDest.split(":")[0], col = srcDest.split(":")[0]+"."+srcCol['colName'].str.cat())
        return joinCondition            
    except Exception as e:
        publishKafka(producer,spark_logger,key,"ERROR","Exception occurred in prepareJoinCodition()")
        publishKafka(producer,spark_logger,key,"ERROR"," The iteration key is :: " + srcDest)
        publishKafka(producer,spark_logger,key,"ERROR","Exception::msg %s" % str(e))
        publishKafka(producer,spark_logger,key,"ERROR",traceback.format_exc())
         


def prepareFilterCodition(srcDest,prcRow,srcColMap,key,producer,spark_logger):
    try:
        publishKafka(producer,spark_logger,key,"INFO","Processing filter condition for "+srcDest)
        for row in prcRow['filterCondition'].split("@") :
            if srcDest.split(":")[0] in row.split(":")[0] :
                #Fetch the column details from src col mapping having same srcID and ColID
                srcCol = srcColMap[(srcColMap['srcId'] == srcDest.split(":")[0]) & (srcColMap['colId'] == int(row.split(":")[1]))]
                return " Where "+srcDest.split(":")[0]+"."+srcCol['colName'].str.cat()+ prcRow['filterCondition'].split("@")[1]
            else :
                return ""            
    except Exception as e:
        publishKafka(producer,spark_logger,key,"ERROR","Exception occurred in prepareFilterCodition()")
        publishKafka(producer,spark_logger,key,"ERROR"," The iteration key is :: " + srcDest)
        publishKafka(producer,spark_logger,key,"ERROR","Exception::msg %s" % str(e))
        publishKafka(producer,spark_logger,key,"ERROR",traceback.format_exc())


                        
def singleSrcPrc(spark,srcMap, schemaMap, destMap, queryMap,filterCondition,key,producer, spark_logger):
    for srcKey, src in srcMap.items():
        try:
            publishKafka(producer,spark_logger,key,"INFO","The processing singleSrcPrc() process for " + srcKey)
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
                ##TODO fieldNames() will be available in verions 2.3.0 onwards ( https://jira.apache.org/jira/browse/SPARK-20090)
                #colName = ','.join(schemaMap[srcKey].fieldNames())
                #Using alternate approach to fieldNames() until then
                fldNames=[]
                for strctFld in schemaMap[srcKey]:
                    fldNames.append(strctFld.jsonValue()['name'])
                colName = ','.join(fldNames)    
                ## Comment the above line till fldNames and uncomment the previous approach in future releases.
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
            publishKafka(producer,spark_logger,key,"ERROR","Exception occurred in singleSrcPrc()")
            publishKafka(producer,spark_logger,key,"ERROR"," The iteration key for srcMap is :: " + srcKey)
            publishKafka(producer,spark_logger,key,"ERROR","Exception::msg %s" % str(e))
            publishKafka(producer,spark_logger,key,"ERROR",traceback.format_exc())

            
        for destKey, dest in destMap.items():
            print(queryMap[destKey])
            print(','.join(queryMap[destKey]))
            try:
                #Fetch value of compression
                if dest.get('compression') is None :
                    compression="none"
                else :
                    compression=dest.get('compression').str.cat() 
                 #Fetch value of numPartitions
                if dest.get('numPartitions') is None :
                    numPartitions=8
                else :
                    numPartitions=dest.get('numPartitions')[0].item()  
                    
                if dest['fileType'].any() == "csv" or dest['fileType'].any() == "json" or dest[
                    'fileType'].any() == "orc" or dest['fileType'].any() == "parquet":
                    spark.sql("select "+','.join(queryMap[destKey])+" from "+destKey.split(":")[0]+filterCondition).coalesce(numPartitions)\
                    .write.mode(dest["mode"].any()).format(dest["fileType"].any()).option("compression",compression)\
                    .save(dest["destLocation"].any() + dest["destId"].any() + "_" + dest["fileType"].any() + "/" + dest["fileType"].any())
                    spark.sql("select "+','.join(queryMap[destKey])+" from "+destKey.split(":")[0]+filterCondition).show(truncate=False)
                    #df.selectExpr(queryMap[destKey]).show(truncate=False)
                elif dest['fileType'].any() == "hivetable":
                    publishKafka(producer,spark_logger,key,"INFO","Publishing data in fromat : "+dest['fileType'].any()+" in mode :"+dest["mode"].any() + " having table name : "+dest["table"].any())
                    spark.sql("select "+','.join(queryMap[destKey])+" from "+destKey.split(":")[0]+filterCondition)\
                        .write.mode(dest["mode"].any()).saveAsTable(dest["table"].any())
                elif dest['fileType'].any() == "jdbcclient":
                    publishKafka(producer,spark_logger,key,"INFO","Publishing data in fromat : "+dest['fileType'].any()+" in mode :"+dest["mode"].any() + " having table name : "+dest["table"].any())
                    spark.sql("select "+','.join(queryMap[destKey])+" from "+destKey.split(":")[0]+filterCondition)\
                        .coalesce(numPartitions).write.format("jdbc").mode(dest["mode"].any())\
                        .option("url", dest["url"].any()).option("driver", dest["driver"].any())\
                        .option("dbtable",dest["table"].any()).option("user",dest["user"].any())\
                        .option("password", dest["password"].any()).save()
                    print("Data inserted successfully for---------- " + destKey )
                elif dest['fileType'].any() == "DataBase":
                    print("TEST107c::")
                    prepareTPTScript(spark,srcMap, schemaMap, destMap, queryMap, producer,spark_logger)


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
                publishKafka(producer,spark_logger,key,"ERROR","Exception occurred in singleSrcPrc()")
                publishKafka(producer,spark_logger,key,"ERROR"," The iteration key for target Map is :: " + destKey)
                publishKafka(producer,spark_logger,key,"ERROR","Exception::msg %s" % str(e))
                publishKafka(producer,spark_logger,key,"ERROR",traceback.format_exc())

    
def multiSrcPrc(spark,srcMap, schemaMap, destMap, queryMap,joinCondition,filterCondition,key, producer,spark_logger):
    for srcKey, src in srcMap.items():
        publishKafka(producer,spark_logger,key,"INFO","In multiSrcPrc() method processing for Src Id " + srcKey)
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
                ##TODO fieldNames() will be available in verions 2.3.0 onwards ( https://jira.apache.org/jira/browse/SPARK-20090)
                #colName = ','.join(schemaMap[srcKey].fieldNames())
                #Using alternate approach to fieldNames() until then
                fldNames=[]
                for strctFld in schemaMap[srcKey]:
                    fldNames.append(strctFld.jsonValue()['name'])
                colName = ','.join(fldNames)    
                ## Comment the above line till fldNames and uncomment the previous approach in future releases.
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
            publishKafka(producer,spark_logger,key,"ERROR","Exception occurred in multiSrcPrc()")
            publishKafka(producer,spark_logger,key,"ERROR"," The iteration key for srcMap is :: " + srcKey)
            publishKafka(producer,spark_logger,key,"ERROR","Exception::msg %s" % str(e))
            publishKafka(producer,spark_logger,key,"ERROR",traceback.format_exc())
            
    queryExpr=""
    #List to keep track of unique destination for publishing
    distinctDest=[]  
    if joinCondition == "NA" and filterCondition == "NA" :
        #Should iterate only once as query is being provided
        for qkey, queryStr in queryMap.items():
            queryExpr=queryStr[0]
    else :
        query="select "    
        for qkey, querylst in queryMap.items():
            query+=','.join(querylst)+","
        queryExpr=query[0:-1]+joinCondition+filterCondition
        
            
    for destKey, dest in destMap.items():
        if destKey.split(":")[1] not in distinctDest :
            distinctDest.append(destKey.split(":")[1])
            publishKafka(producer,spark_logger,key,"INFO","Publishing the records for Dest Id :: "+destKey.split(":")[1])
            #Fetch value of compression
            if dest.get('compression') is None :
                compression="none"
            else :
                compression=dest.get('compression').str.cat() 
            #Fetch value of numPartitions
            if dest.get('numPartitions') is None :                 
                numPartitions=8
            else :
                numPartitions=dest.get('numPartitions')[0].item()
                 
            try:
                publishKafka(producer,spark_logger,key,"INFO",":::::Executing Query::::::"+queryExpr)
                dfWrite=spark.sql(queryExpr)                
                if dest['fileType'].any() == "csv" or dest['fileType'].any() == "json" or dest['fileType'].any() == "orc" or dest['fileType'].any() == "parquet":
                    publishKafka(producer,spark_logger,key,"INFO","Publishing data in fromat : "+dest['fileType'].any()+" in mode :"+dest["mode"].any() + " at "+dest["destLocation"].any() + dest["destId"].any() + "_" + dest["fileType"].any() + "/" + dest[
                                "fileType"].any())
                    dfWrite.coalesce(numPartitions).write.mode(dest["mode"].any()).format(dest["fileType"].any())\
                    .option("compression",compression)\
                    .save(dest["destLocation"].any() + dest["destId"].any() + "_" + dest["fileType"].any() + "/" + dest["fileType"].any())   
                    dfWrite.show(truncate=False)                 
                    #spark.sql(query[0:-1]+joinCondition+filterCondition).show(truncate=False)
                elif dest['fileType'].any() == "hivetable":
                    publishKafka(producer,spark_logger,key,"INFO","Publishing data in fromat : "+dest['fileType'].any()+" in mode :"+dest["mode"].any() + " having table name : "+dest["table"].any())
                    dfWrite.write.mode(dest["mode"].any()).saveAsTable(dest["table"].any())
                elif dest['fileType'].any() == "jdbcclient":
                    publishKafka(producer,spark_logger,key,"INFO","Publishing data in fromat : "+dest['fileType'].any()+" in mode :"+dest["mode"].any() + " having table name : "+dest["table"].any())
                    dfWrite.coalesce(numPartitions).write.format("jdbc").mode(dest["mode"].any())\
                        .option("url", dest["url"].any()).option("driver", dest["driver"].any())\
                        .option("dbtable",dest["table"].any()).option("user",dest["user"].any())\
                        .option("password", dest["password"].any()).save()
                elif dest['fileType'].any() == "DataBase":
                    print("TEST107c::")
                    prepareTPTScript(spark,srcMap, schemaMap, destMap, queryMap,producer, spark_logger)   
                         
            except Exception as e:
                publishKafka(producer,spark_logger,key,"ERROR","Exception occurred in multiSrcPrc()")
                publishKafka(producer,spark_logger,key,"ERROR"," The iteration key for target Map is :: " + destKey)
                publishKafka(producer,spark_logger,key,"ERROR","Exception::msg %s" % str(e))
                publishKafka(producer,spark_logger,key,"ERROR",traceback.format_exc())
        
        publishKafka(producer,spark_logger,key,"INFO","Published the records for Dest Ids :: "+' '.join(distinctDest))
        
def prepareMeta(sprkSession, prcRow,key,producer,spark_logger):
    possibleError=""
    #key=logKey(sprkSession, prcRow['prcId'])
    #spark_logger = logg.Log4j(sprkSession,key)
    #spark_logger.warn("_________________Started processing process Id : " + prcRow['prcId'] + " : ____________________")
    try:
        publishKafka(producer,spark_logger,key,"INFO","Started processing process Id : "+prcRow['prcId'])
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
                fields = fetchSchema(srcColMap[srcColMap['srcId'] == mapRow['srcId']],key,producer, spark_logger)
                schema = StructType(fields)
                schemaMap[srcDest] = schema
                # Fetch source and destination details
                src = pd.read_json(config.get('DIT_setup_config', 'srcDetails') + 'src_' + mapRow['srcId'] + '.json')
                dest = pd.read_json(config.get('DIT_setup_config', 'destDetails') + 'dest_' + mapRow['destId'] + '.json')
                srcMap[srcDest] = src[src['srcId'] == mapRow['srcId']]
                destMap[srcDest] = dest[dest['destId'] == mapRow['destId']]
                #Set join condition  
                if prcRow.get('joinCol') is not None:              
                    joinCondition=prepareJoinCodition(joinCondition,srcDest,prcRow,srcColMap,key,producer,spark_logger)
                #TODO device a logic to seperately write filter queries 
                if prcRow.get('filterCondition') is not None:     
                    filterCondition+=prepareFilterCodition(srcDest, prcRow, srcColMap,key,producer,spark_logger)
        #Identify the process mapping     
        mapping=findMapping(mapTab.srcId.nunique(),mapTab.destId.nunique(),producer,spark_logger)
        #Process data 
        processData(sprkSession,mapping, srcMap, schemaMap, destMap, queryMap,joinCondition,filterCondition,key,producer, spark_logger)
    except Exception as e:
        publishKafka(producer,spark_logger,key,"ERROR","Exception occurred in prepareMeta()")
        publishKafka(producer,spark_logger,key,"ERROR"," The exception occurred for process ID :: " + prcRow['prcId'])
        publishKafka(producer,spark_logger,key,"ERROR"," The possible errors can be "+possibleError)
        publishKafka(producer,spark_logger,key,"ERROR","Exception::msg %s" % str(e))
        publishKafka(producer,spark_logger,key,"ERROR",traceback.format_exc())


def executeQuery(sprkSession, prcRow,key,producer,spark_logger):
    possibleError=""
    try:
        publishKafka(producer,spark_logger,key,"INFO","Started processing process Id : "+prcRow['prcId'] + " with SQL query provided")
        queryMap = {}
        schemaMap = {}
        srcMap = {}
        destMap = {}
        #joinCondition=" from {tab1} inner join {tab2} on {col1} = {col2}"
        joinCondition="NA"
        filterCondition= "NA"
        # Fetch process Id specific mapping file
        maps = pd.read_json(config.get('DIT_setup_config', 'prcMapping') + 'colMapping_' + prcRow['mapId'] + '.json')
        mapTab = maps[maps['mapId'] == prcRow['mapId']]
        srclst = []
        deslst = []
        for src in mapTab['srcId'].tolist():
            srclst+=src
        for dest in mapTab['destId'].tolist():
            deslst+=dest
        srcDestSet=set(itertools.product(srclst,deslst))
        #print(srcDestSet)
        for row in srcDestSet:
            srcDest = row[0] + ":" + row[1]
            # Fetch source and destination column mapping files with respect to each source and column 
            srcColMap = pd.read_json(config.get('DIT_setup_config', 'srcCols') + 'srcCols_' + row[0] + '.json')
            destColMap = pd.read_json(config.get('DIT_setup_config', 'destCols') + 'destCols_' + row[1] + '.json')
            # query.append(srcCol['colName'].str.cat()+" as "+destCol['colName'].str.cat())
         

            ## Fetch schema of the sources
            if srcDest not in schemaMap:
                fields = fetchSchema(srcColMap[srcColMap['srcId'] == row[0]],key,producer, spark_logger)
                schema = StructType(fields)
                schemaMap[srcDest] = schema
                # Fetch source and destination details
                src = pd.read_json(config.get('DIT_setup_config', 'srcDetails') + 'src_' + row[0] + '.json')
                dest = pd.read_json(config.get('DIT_setup_config', 'destDetails') + 'dest_' + row[1] + '.json')
                srcMap[srcDest] = src[src['srcId'] == row[0]]
                destMap[srcDest] = dest[dest['destId'] == row[1]]
                #Add Query
                queryMap[srcDest] =  mapTab['query'] 
        #Identify the process mapping     
        mapping=findMapping(len(srclst),len(deslst),producer,spark_logger)
        #Process data 
        processData(sprkSession,mapping, srcMap, schemaMap, destMap, queryMap,joinCondition,filterCondition,key,producer, spark_logger)
    except Exception as e:
        publishKafka(producer,spark_logger,key,"ERROR","Exception occurred in executeQuery()")
        publishKafka(producer,spark_logger,key,"ERROR"," The exception occurred for process ID :: " + prcRow['prcId'])
        publishKafka(producer,spark_logger,key,"ERROR"," The possible errors can be "+possibleError)
        publishKafka(producer,spark_logger,key,"ERROR","Exception::msg %s" % str(e))
        publishKafka(producer,spark_logger,key,"ERROR",traceback.format_exc())

                
def fetchSchema(srcCols,key, producer,spark_logger):
    try:
        publishKafka(producer,spark_logger,key,"INFO","Fetching schema values for SRC Id " + srcCols['srcId'].any())
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
        publishKafka(producer,spark_logger,key,"ERROR","Exception occurred in fetchSchema()")
        publishKafka(producer,spark_logger,key,"ERROR"," The exception occurred for Src Id :: " + srcCols['srcId'].str.cat())
        publishKafka(producer,spark_logger,key,"ERROR","Exception::msg %s" % str(e))
        publishKafka(producer,spark_logger,key,"ERROR",traceback.format_exc())


def processData(spark,mapping, srcMap, schemaMap, trgtMap, queryMap,joinCondition,filterCondition, key,producer,spark_logger):
    # TODO find alternative to any and restrict it to one row using tail head etc
    publishKafka(producer,spark_logger,key,"INFO","The process mapping of the current process is :: " +mapping)
    if mapping== "One_to_One" or mapping== "One_to_Many":
        singleSrcPrc(spark,srcMap, schemaMap, trgtMap, queryMap,filterCondition,key,producer, spark_logger)
    elif mapping == "Many_to_One"  :
        multiSrcPrc(spark,srcMap, schemaMap, trgtMap, queryMap,joinCondition,filterCondition,key,producer, spark_logger)
    elif mapping == "Many_to_Many" :
        print("in "+mapping)



def processFiles(argTuple):
    # spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    try:
        prc = pd.read_json(argTuple[0])
        #instantiate Kafka Producer
        producer = KafkaProducer(bootstrap_servers=config.get('DIT_Kafka_config', 'KAFKA_BROKERS').split(','))
        for prcIdx, prcRow in prc[prc['isActive'] == "True"].iterrows():
            key=logKey(argTuple[1], prcRow['prcId'])
            spark_logger = logg.Log4j(argTuple[1],key)
            if prcRow.get('queryProvided') == "True" :
                executeQuery(argTuple[1], prcRow,key,producer,spark_logger)                
            else :
                prepareMeta(argTuple[1], prcRow,key,producer,spark_logger)
                    
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
    for dir, root, files in os.walk(config.get('DIT_setup_config', 'prcDetails')):
        matches = re.finditer(r'{0}'.format(prcPattern), ' '.join(files), re.MULTILINE)
        for matchNum, match in enumerate(matches):
            prcList.append(os.path.join(dir, match.group()))
    
    threadPool = ThreadPool(pool)
    print("List of process files to be processed are :: \n", prcList)
    spark = pyspark.sql.SparkSession.builder.appName("DataIngestion").enableHiveSupport().getOrCreate()
    
    threadPool.map(processFiles, zip(prcList, itertools.repeat(spark.newSession())))
    # spark.stop()





#if __name__ == "__main__":
#    prcs="prc_PrcId_[0-9].json"
#    pool=3
#    sys.exit(main('C:\\Users\\sk250102\\Documents\\Teradata\\DIT\\DataIngestionTool\\config\\config.cnf', prcs,pool))


