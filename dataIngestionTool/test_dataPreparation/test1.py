
import sys
sys.path.append('../') 
import dataPrepartion.commonUtil as comUtil
try:
    import pyspark
except:
    import findspark
    findspark.init()
    
from  pyspark.sql.functions import input_file_name
    

loc ="C:/Users/sk250102/Documents/Teradata/DIT/DataIngestionTool/dataIngestionTool/test_dataPreparation/TestFiles/TestCsvToCsv/source/"
dest= "C:/Users/sk250102/Documents/Teradata/DIT/DataIngestionTool/dataIngestionTool/test_dataPreparation/config/input"
arch= "C:/Users/sk250102/Documents/Teradata/DIT/DataIngestionTool/dataIngestionTool/test_dataPreparation/TestFiles/TestCsvToCsv/archive"

comUtil.moveToHDFS(loc,dest+"/serc1")

spark = pyspark.sql.SparkSession.builder.appName("Test_Csv_To_Csv").enableHiveSupport().getOrCreate()
df = spark.read.format("csv")\
    .option("header", "True").option("delimiter", ",")\
    .load(dest+"/serc1")
    
df.show()  

comUtil.moveAcrossHDFS(dest+"/serc1/",arch+"/arch1")
#df.registerTempTable("x")

#df.withColumn("filename",input_file_name()).groupby("filename").select("filename").show(truncate=False)

#df.show() 


#spark.sql("select metadata from x").show()
#spark.sql("select  metadata.path from x").show()
#spark.sql("select  metadata.path, metadata.size from x").show()  

