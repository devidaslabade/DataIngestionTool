import os
import ftplib
import datetime
import traceback
import shutil
import json
from jsonschema import validate

def validateDataWithSchema(row, jsonSchemaMap):
    print("index:: ")
    updateDF = []
    for x in row:
        try:
            data = json.loads(x)
            #print(data.collect())
            validate(data, jsonSchemaMap)
            data["valid"] = "Y"
            data["errMsg"] = ""
            updateDF.append(data)
            print("OK")
        except Exception as e:
            print(str(e.message))
            data["valid"] = "N"
            data["errMsg"] = str(e.message)
            updateDF.append(data)
    print("Finally")
    print(updateDF)
    return updateDF


#delete this method from dataIngestion
def publishKafka(producer,topic,spark_logger,prcKey,logLevel,msg):
    try:
        if logLevel == "INFO" or logLevel == "WARN":
            spark_logger.warn(msg)
        else :
            spark_logger.error(msg)     
        jsonString = {"Timestamp":str(datetime.datetime.now()),"LogLevel": logLevel,"LogMsg":msg}
        producer.send(topic, key=prcKey.encode('utf-8'), value=json.dumps(jsonString).encode('utf-8'))
    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in publishKafka() ________________")
        print("Exception::msg %s" % str(e))
        print(traceback.format_exc())

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
        
        
def dirPathGen(dirType,srcDestId,producer,config,spark_logger,key):
    try:       
        loc= config.get('DIT_setup_config', 'processingZone')+dirType+"/"+key+"/"+srcDestId.replace(":","_")+"/"
        return loc
    except Exception as ex:
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR","Exception occurred in dirPathGen()")
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR"," The exception occurred for Src Id :: " )
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR","Exception::msg %s" % str(ex))
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR",traceback.format_exc())



def moveDataToProcessingZone(config,srcMap,key,producer,spark_logger):
    try :
        isSuccess = False
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"INFO","Moving data from landing zone to processing zone")
        for srcKey, src in srcMap.items():
            if src.get('srcType').str.cat().lower() == "local".lower():
                isSuccess=moveToHDFS("input",srcKey,src['srcLocation'].any(),config,key,producer,spark_logger)
            elif src.get('srcType').str.cat().lower() == "hdfs".lower(): 
                isSuccess=moveAcrossHDFS("input",srcKey,src['srcLocation'].any(),config,key,producer,spark_logger)
            else :
                #TODO add remote,ftp,scp options
                print ("srcType not mentioned")     
        return isSuccess
    except Exception as ex :
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR","Exception occurred in moveDataToProcessingZone()")
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR"," The exception occurred for Src Id :: " )
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR","Exception::msg %s" % str(ex))
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR",traceback.format_exc())
        return False

def moveData(subFolderName,config,srcMap,key,producer,spark_logger):
        try :
            
            for srcKey, src in srcMap.items():
                srcLocation= dirPathGen("input",srcKey,producer,config,spark_logger,key)
                destLocation= dirPathGen(subFolderName,srcKey,producer,config,spark_logger,key)
                moveAcrossHDFS(srcLocation,destLocation,producer,config,spark_logger,key)
            return True
        except Exception as ex :
            publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR","Exception occurred in moveData()")
            publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR"," The exception occurred for Src Id :: " )
            publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR","Exception::msg %s" % str(ex))
            publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR",traceback.format_exc())  
            return Flase
            
def moveToHDFS(dirType,srcDestId,localsrc,config,key,producer,spark_logger):
    try:
       destinationPath= dirPathGen(dirType,srcDestId,producer,config,spark_logger,key)
       #config.get('DIT_setup_config', 'processingZone')+"input/"+key+"/"
       publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"INFO","moving files from local system :: "+localsrc+" to :: "+destinationPath)
       #return os.system("hdfs dfs -copyFromLocal file://home/hadoop/data1.txt hdfs:/data1/data1.txt")
       #return os.system("hadoop fs -moveFromLocal {0} {1}".format(localsrc,destinationPath) )
       if not os.path.exists(os.path.dirname(destinationPath)):
           os.makedirs(os.path.dirname(destinationPath))
           #os.mkdir(destinationPath)
       ret=shutil.move(localsrc,destinationPath)
       print("the return value :: "+ret)
       #os.system("scp API-0.0.1-SNAPSHOT.war user@serverIp:/path")
       return True
    except Exception as ex:
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR","Exception occurred in moveToHDFS()")
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR"," The exception occurred for Src Id :: " )
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR","Exception::msg %s" % str(ex))
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR",traceback.format_exc())
        return False

#TODO complete this method
def moveAcrossHDFS(srcLocation,destLocation,producer,config,spark_logger,key):
    try:
       publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"INFO","moving files from HDFS :: "+srcLocation+" to :: "+destLocation)
       #return os.system("hdfs dfs -copyFromLocal file://home/hadoop/data1.txt hdfs:/data1/data1.txt")
       #os.system("hadoop fs -mv {0} {1}".format(srcPath,destinationPath) )
       retv=shutil.move(srcLocation,destLocation)
       print("tthe ret is "+retv)
       #os.system("scp API-0.0.1-SNAPSHOT.war user@serverIp:/path")
       return True
    except Exception as ex:
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR","Exception occurred in moveAcrossHDFS()")
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR"," The exception occurred for Src Id :: " )
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR","Exception::msg %s" % str(ex))
        publishKafka(producer,config.get('DIT_Kafka_config', 'TOPIC'),spark_logger,key,"ERROR",traceback.format_exc())
        return False

def publishSCP(fileAbsPathName,user,password,serverDetails,destinationPath):
    try:
       return os.system("scp FILE USER@SERVER:PATH")
   #os.system('scp "%s" "%s:%s"' % (fileAbsPathName, remotehost, remotefile) )
   #os.system("scp API-0.0.1-SNAPSHOT.war user@serverIp:/path")
    except Exception as e:
      print (str(datetime.datetime.now()) + "____________Spark Context creation Failed________________")   

      
def uploadFileFTP(server, username, password,sourceFilePath,destinationFileName,destinationDirectory ):
    #print("the details are : server : %s username : %s password : %s"% (server, username, password))
    myFTP = ftplib.FTP(server, username, password)
    print ("The present working directory is ::"+myFTP.pwd())
    # Changing Working Directory
    myFTP.cwd(destinationDirectory)
    print ("Changing the working directory to ::"+myFTP.pwd())
    print("The source file path is ::"+sourceFilePath)
    if os.path.isfile(sourceFilePath):
        fh = open(sourceFilePath, 'rb')
        print ("The file name is "+destinationFileName)
        myFTP.storbinary('STOR %s' % destinationFileName, fh)
        fh.close()
        myFTP.quit()
        print("File has been successfully processed")
    else:
        print ("Source File does not exist")     