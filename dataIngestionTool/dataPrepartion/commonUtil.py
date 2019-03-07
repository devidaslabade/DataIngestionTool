import os
import json
import ftplib
import datetime
import traceback
#test
import shutil

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


def moveToHDFS(localsrc,destinationPath):
    try:
       #return os.system("hdfs dfs -copyFromLocal file://home/hadoop/data1.txt hdfs:/data1/data1.txt")
       #return os.system("hadoop fs -moveFromLocal {0} {1}".format(localsrc,destinationPath) )
       ret=shutil.move(localsrc,destinationPath)
       print(ret)
   #os.system("scp API-0.0.1-SNAPSHOT.war user@serverIp:/path")
    except Exception as e:
      print (str(datetime.datetime.now()) + "____________Spark Context creation Failed________________")  
      print("Exception::msg %s" % str(e))
      print(traceback.format_exc())

def moveAcrossHDFS(srcPath,destinationPath):
    try:
       #return os.system("hdfs dfs -copyFromLocal file://home/hadoop/data1.txt hdfs:/data1/data1.txt")
       #os.system("hadoop fs -mv {0} {1}".format(srcPath,destinationPath) )
       shutil.move(srcPath,destinationPath)
   #os.system("scp API-0.0.1-SNAPSHOT.war user@serverIp:/path")
    except Exception as e:
      print (str(datetime.datetime.now()) + "____________Spark Context creation Failed________________")  
      print("Exception::msg %s" % str(e))
      print(traceback.format_exc())

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
                 