# -*- coding: utf-8 -*-
import sys
sys.path.append('../')
import unittest
import sqlite3
import warnings
import importlib
from configparser import ConfigParser
import os
import shutil

try:
    import pyspark
except:
    import findspark
    findspark.init()
from pyspark.sql.types import *   

from pyspark.sql.functions import regexp_extract, col

# instantiate config Parser
config = ConfigParser()
config.read('config\\config.cnf')

def execute_valid_process():
        module = importlib.import_module('dataPrepartion.dataIngestion')
        print("+++++++++++++++++++++Executing Test cases with source as Fixed Width Files+++++++++++++++++++++++")
        prcs = "(prc_PrcId_18.json)"
        #prcs = "(prc_PrcId_16.json|prc_PrcId_17.json|prc_PrcId_18.json|prc_PrcId_19.json)"
        pool = 3
        module.main('config\\config.cnf', prcs, pool)

def delete_dest_dir():
    if os.path.exists(config.get('DIT_TEST_CASE_config', 'DEST_LOC_FXDWDTH')):
        shutil.rmtree(config.get('DIT_TEST_CASE_config', 'DEST_LOC_FXDWDTH'))   
    
    if os.path.isfile(config.get('DIT_TEST_CASE_config', 'DB_LOC_FXDWDTH_JDBC')):
        os.remove(config.get('DIT_TEST_CASE_config', 'DB_LOC_FXDWDTH_JDBC'))  
    
    if os.path.exists(config.get('DIT_TEST_CASE_config', 'DB_LOC_FXDWDTH_WAREHOUSE')):
            shutil.rmtree(config.get('DIT_TEST_CASE_config', 'DB_LOC_FXDWDTH_WAREHOUSE'))
        
    if os.path.exists(config.get('DIT_TEST_CASE_config', 'DB_LOC_FXDWDTH_DERBY')):
            shutil.rmtree(config.get('DIT_TEST_CASE_config', 'DB_LOC_FXDWDTH_DERBY'),ignore_errors=True)  
            
     #Move test data files to landing folder    
    if os.path.exists(config.get('DIT_TEST_CASE_config', 'FXDWDTH_FILE_LANDING_FOLDER')):
        shutil.rmtree(config.get('DIT_TEST_CASE_config', 'FXDWDTH_FILE_LANDING_FOLDER'))
    shutil.copytree(config.get('DIT_TEST_CASE_config', 'FXDWDTH_FILE_DATA_PREP_FOLDER'), config.get('DIT_TEST_CASE_config', 'FXDWDTH_FILE_LANDING_FOLDER'))        
        

class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        warnings.simplefilter('ignore', category=ImportWarning)
        warnings.simplefilter('ignore', category=DeprecationWarning)
        delete_dest_dir()
        os.environ["SPARK_CONF_DIR"] = config.get('DIT_TEST_CASE_config', 'SPARK_CONF_DIR_FXDWDTH')
        cls.spark = pyspark.sql.SparkSession.builder.appName("Test_Fixed_Width")\
                    .enableHiveSupport().getOrCreate()
        execute_valid_process()
 
    
    def setUp(self):
        print("setup")

     
    def tearDown(self):
        print("tearDown")     
    
    @classmethod
    def tearDownClass(cls):
        """
        Delete the database
        """
        cls.spark.stop()
        delete_dest_dir()
        print("tearDownClass")      


    '''
    Read from a file, filter the data, transform data of one of the columns using SQL function, save the output in compressed file format
    '''
    @unittest.skip("demonstrating skipping") 
    def test_PrcId_16(self):
        """
        Test case for checking functionality of 
        

        
        """
        print("Validating test result of PrcId_16")  
        isValid=False
        destDir=config.get('DIT_TEST_CASE_config', 'DEST_LOC_FXDWDTH').strip()+"/DestId_16_json/json/"
        observedDF = self.spark.read.json(destDir)
        obsCount=observedDF.count()
        print("The count of records at destination location is :: "+str(obsCount))
        for file in os.listdir(destDir):
            if file.endswith(".bz2") and obsCount== 10:
                print("The file is bzip compressed :: "+file)                
                isValid=True
        self.assertTrue(isValid)




    '''
    Read from files, perform inner join, filter records, and then add an extra column with some default/constant value or SQL function.
    '''
    @unittest.skip("demonstrating skipping")     
    def test_PrcId_17(self):
        """
        Test case for checking functionality of 
        

        
        """
        print("Validating test result of PrcId_17")
        # Read from Hive
        df_load = self.spark.sql('select cat_id from fin_tab_dest17 where status="Resolved"')
        df_load.show()
        retVal=df_load.collect()
        self.assertEqual(815681323, retVal[0]['cat_id'])



        
        
    #@unittest.skip("demonstrating skipping")    
    def test_PrcId_18(self):
        """
        Test case for checking functionality of 
        

        
        """
        print("Validating test result of PrcId_18")
        # Read from Hive
        df_load = self.spark.sql('select assigned_to from fin_tab_dest18 where status="Assigned"')
        df_load.show()
        retVal=df_load.collect()
        self.assertEqual('Sridar', retVal[0]['assigned_to'])
     
        
          

    @unittest.skip("demonstrating skipping")    
    def test_PrcId_19(self):
        """
        Test case for checking functionality of 
        

        
        """
        print("Validating test result of PrcId_19")
        conn = sqlite3.connect(config.get('DIT_TEST_CASE_config', 'DB_LOC_FXDWDTH_JDBC'))
        cursor = conn.cursor()
        # Read from JDBC Source
        resultSet=cursor.execute('select cnt_status from Dest_19 where status="Open"').fetchall()        
        cursor.close()
        conn.close()
        print(resultSet)
        self.assertEqual(1, resultSet[0][0])





if __name__ == '__main__':
    unittest.main(warnings='ignore')