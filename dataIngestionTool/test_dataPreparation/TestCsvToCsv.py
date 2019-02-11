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
# instantiate config Parser
config = ConfigParser()
config.read('config/config.cnf')

def execute_valid_process():
        module = importlib.import_module('dataPrepartion.dataIngestion')
        print(module)
        prcs = "(prc_PrcId_1.json|prc_PrcId_2.json|prc_PrcId_3.json|prc_PrcId_10.json|prc_PrcId_11.json)"
        #prcs = "(prc_PrcId_2.json)"
        pool = 3
        module.main('config/config.cnf', prcs, pool)
        
def delete_dest_dir():
    shutil.rmtree('TestFiles/TestCsvToCsv/destLoc/', ignore_errors=True)
    if os.path.exists(config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV_WAREHOUSE')):
            shutil.rmtree(config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV_WAREHOUSE'), ignore_errors=True)
        
    if os.path.exists(config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV_DERBY')):
            shutil.rmtree(config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV_DERBY'), ignore_errors=True)  
        
    if os.path.isfile(config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV')):
            os.remove(config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV'))   

class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        warnings.simplefilter('ignore', category=ImportWarning)
        warnings.simplefilter('ignore', category=DeprecationWarning)
        os.environ["SPARK_CONF_DIR"] = config.get('DIT_TEST_CASE_config', 'SPARK_CONF_DIR_CSV')
        delete_dest_dir()
        #TestFiles\\TestCsvToCsv\\destLoc\\
        execute_valid_process()

        cls.spark = pyspark.sql.SparkSession.builder.appName("Test_Csv_To_Csv").enableHiveSupport().getOrCreate()
  
    
    def setUp(self):
        print("setup")     

     
    def tearDown(self):
        print("tearDown")     
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        delete_dest_dir()
        print("tearDownClass")      

    '''
    Read from files, perform inner join, filter records, and then add an extra column with some default/constant value or SQL function.
    '''
    #@unittest.skip("demonstrating skipping")    
    def test_PrcId_1(self):
        print("Validating test result of PrcId_1")
        observedDF = self.spark.read.json("TestFiles/TestCsvToCsv/destLoc/DestId_1_json/json/")
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
        destDir="TestFiles/TestCsvToCsv/destLoc/DestId_2_json/json/"
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
        observedDF = self.spark.read.json("TestFiles/TestCsvToCsv/destLoc/DestId_3_json/json/")
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
        conn = sqlite3.connect(config.get('DIT_TEST_CASE_config', 'DB_LOC_CSV'))
        cursor = conn.cursor()
        # Read from JDBC Source
        resultSet=cursor.execute('select cat_name from Dest_11 where dept_name="Fitness" and cat_id=2').fetchall()        
        cursor.close()
        conn.close()
        print(resultSet)
        self.assertEqual('Soccer', resultSet[0][0])





if __name__ == '__main__':
    unittest.main()