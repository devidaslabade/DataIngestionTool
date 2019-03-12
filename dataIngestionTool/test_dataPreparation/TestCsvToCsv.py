# -*- coding: utf-8 -*-
import sys
sys.path.append('../') 
import os
import unittest
import sqlite3
import warnings
import importlib
import shutil
from configparser import ConfigParser
try:
    import pyspark
except:
    import findspark
    findspark.init()  
    
from pyspark.sql.functions import col
from pyspark.conf import SparkConf     


def execute_valid_process():
        module = importlib.import_module('dataPrepartion.dataIngestion')
        print("+++++++++++++++++++++Executing Test cases with source as Delimited Text +++++++++++++++++++++++")
        prcs = "(prc_PrcId_1.json|prc_PrcId_2.json|prc_PrcId_3.json|prc_PrcId_10.json|prc_PrcId_11.json|prc_PrcId_20.json|prc_PrcId_21.json|prc_PrcId_22.json|prc_PrcId_23.json)"
        #prcs = "(prc_PrcId_23.json)"
        pool = 3
        module.main('config/config.cnf', prcs, pool)

def delete_dest_dir(cls):
    if os.path.exists(cls.config.get('DIT_TEST_CASE_config', 'DEST_LOC_CSV')):
        shutil.rmtree(cls.config.get('DIT_TEST_CASE_config', 'DEST_LOC_CSV'))   
    
    if os.path.isfile(cls.config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV_JDBC')):
        os.remove(cls.config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV_JDBC'))  
    
    if os.path.exists(cls.config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV_WAREHOUSE')):
            shutil.rmtree(cls.config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV_WAREHOUSE'))
        
    if os.path.exists(cls.config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV_DERBY')):
            shutil.rmtree(cls.config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV_DERBY'),ignore_errors=True)  
        
 

class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        warnings.simplefilter('ignore', category=ImportWarning)
        warnings.simplefilter('ignore', category=DeprecationWarning)
        # instantiate config Parser
        cls.config = ConfigParser()
        cls.config.read('config/config.cnf')
        os.environ["SPARK_CONF_DIR"] = cls.config.get('DIT_TEST_CASE_config', 'SPARK_CONF_DIR_CSV')
        delete_dest_dir(cls)
        modulePath = os.path.join(os.path.abspath("../dataPrepartion"),'common_utils.py')
        print("Adding module py file ::"+modulePath+" to Spark context")
        cls.spark = pyspark.sql.SparkSession.builder.appName("Test_Csv_To_Csv").enableHiveSupport().getOrCreate()
        cls.spark.sparkContext.addPyFile(modulePath)
        execute_valid_process()



    def setUp(self):
        print("setup")     

     
    def tearDown(self):
        print("tearDown")     
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        delete_dest_dir(cls)
        print("tearDownClass")      

    '''
    Read from files, perform inner join, filter records, and then add an extra column with some default/constant value or SQL function.
    '''
    #@unittest.skip("demonstrating skipping")    
    def test_PrcId_1(self):
        print("Validating test result of PrcId_1")
        observedDF = self.spark.read.json(self.config.get('DIT_TEST_CASE_config', 'DEST_LOC_CSV').strip()+"/DestId_1_json/json/")
        obsCount=observedDF.count()
        filteredCount=observedDF.filter("cat_dpt_id = '2_X_Y_Z' and dept_name = 'Fitness' ").count()
        print("The count of records at destination location is :: "+str(obsCount))
        print("The count of filtered records is :: "+str(filteredCount))
        self.assertEqual(obsCount, filteredCount)




    '''
    Read from a file, filter the data, transform data of one of the columns using SQL function, save the output in compressed file format
    '''
    #@unittest.skip("demonstrating skipping")   
    def test_PrcId_2(self):
        print("Validating test result of PrcId_2")
        isValid=False
        destDir=self.config.get('DIT_TEST_CASE_config', 'DEST_LOC_CSV').strip()+"/DestId_2_json/json/"
        observedDF = self.spark.read.json(destDir)
        obsCount=observedDF.count()
        print("The count of records at destination location is :: "+str(obsCount))
        for file in os.listdir(destDir):
            if file.endswith(".bz2") and obsCount== 1:
                print("The file is bzip compressed :: "+file)                
                isValid=True
        self.assertTrue(isValid)



    '''
    Read from files, perform inner join, filter records, and then add an extra column with some default/constant value or SQL function.
    '''
    #@unittest.skip("demonstrating skipping")
    def test_PrcId_3(self):
        print("Validating test result of PrcId_3")
        observedDF = self.spark.read.json(self.config.get('DIT_TEST_CASE_config', 'DEST_LOC_CSV').strip()+"/DestId_3_json/json/")
        obsCount=observedDF.show()
        filteredCount=observedDF.filter("category_department_id = 8 and cnt_cat = 10 ").count()
        #print("The count of records at destination location is :: "+str(obsCount))
        print("The count of filtered records is :: "+str(filteredCount))
        self.assertEqual(1, filteredCount)



        
        
    #@unittest.skip("demonstrating skipping")    
    def test_PrcId_10(self):
        print("Validating test result of PrcId_10")
        # Read from Hive
        df_load = self.spark.sql('select cat_name from fin_tab_dest10 where dept_name="Fitness" and cat_id=7')
        df_load.show()
        retVal=df_load.collect()
        #print(retVal[0].job)
        self.assertEqual("Hockey", retVal[0]['cat_name'])
     
        
          
    #@unittest.skip("demonstrating skipping") 
    def test_PrcId_11(self):
        print("Validating test result of PrcId_11")
        conn = sqlite3.connect(self.config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV_JDBC'))
        cursor = conn.cursor()
        # Read from JDBC Source
        resultSet=cursor.execute('select cat_name from Dest_11 where dept_name="Fitness" and cat_id=2').fetchall()        
        cursor.close()
        conn.close()
        print(resultSet)
        self.assertEqual('Soccer', resultSet[0][0])

    #@unittest.skip("demonstrating skipping")
    def test_PrcId_20(self):
        print("Validating test result of PrcId_20")
        flag = False
        sFlag = False
        tFlag = False
        observedDF = self.spark.read.json(self.config.get('DIT_TEST_CASE_config', 'DEST_LOC_CSV').strip() + "/DestId_20_json/json/")
        srcDF = self.spark.read.format("csv").option("header","true").option("inferSchema","true").option("quote","?").load(self.config.get('DIT_TEST_CASE_config', 'SRC_LOC_CSV'))

        joinDF = observedDF.join(srcDF, observedDF.cat_id == srcDF.c0).select(observedDF.cat_name, srcDF.c2)
        print(joinDF.show())
        #Second scenario testing
        resDF = joinDF.filter("cat_name != c2")
        if (resDF.count() == 0):
            flag = True
        print("The count of filtered records is :: " + str(resDF.count()))

        value = observedDF.filter("cat_id = 5 and cat_name = 'Lacrosse,123'")
        print(value.count())
        if(value.count() == 1):
            sflag = True

        print("flag value::" + str(flag))
        print("sflag value::" + str(sflag))
        if(flag and sflag):
            tFlag = True
            print("Inside")
        print("Final value::"+str(tFlag))
        self.assertTrue(tFlag)

    #@unittest.skip("demonstrating skipping")
    def test_PrcId_21(self):
        print("Validating test result of PrcId_21")
        observedDF = self.spark.read.json(self.config.get('DIT_TEST_CASE_config', 'DEST_LOC_CSV').strip() + "/DestId_21_json/json/")
        filteredCount = observedDF.count()
        print("The count of filtered records is :: " + str(filteredCount))
        self.assertEqual(4, filteredCount)

    #@unittest.skip("demonstrating skipping")
    def test_PrcId_22(self):
        print("Validating test result of PrcId_22")
        observedDF = self.spark.read.format("csv").option("delimiter","|").load(self.config.get('DIT_TEST_CASE_config', 'DEST_LOC_CSV').strip() + "/DestId_22_csv/csv/")
        filteredCount = len(observedDF.columns)
        print("The count of filtered records is :: " + str(filteredCount))
        self.assertEqual(3, filteredCount)

    #@unittest.skip("demonstrating skipping")
    def test_PrcId_23(self):
        print("Validating test result of PrcId_23")
        flag = False
        validFlag = False
        invalidFlag = False
        observedvalidDF = self.spark.read.format("csv").load(self.config.get('DIT_TEST_CASE_config', 'DEST_LOC_CSV').strip() + "/DestId_23_csv/csv/")
        filteredValidCount = observedvalidDF.count()
        print("The count of filtered valid records is :: " + str(filteredValidCount))
        if(filteredValidCount == 2):
            validFlag = True

        observedinvalidDF = self.spark.read.format("csv").load(self.config.get('DIT_TEST_CASE_config', 'DEST_LOC_CSV').strip() + "/DestId_23_csv/csv_INVALID/")
        filteredInvalidCount = observedinvalidDF.count()
        print("The count of filtered invalid records is :: " + str(filteredInvalidCount))
        if (filteredInvalidCount == 6):
            invalidFlag = True

        if(validFlag and invalidFlag):
            flag = True

        print("Final value of test case result :: " + str(flag))
        self.assertTrue(flag)


if __name__ == '__main__':
    unittest.main()