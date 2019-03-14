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
    

def execute_valid_process():
        module = importlib.import_module('dataPrepartion.dataIngestion')
        print("+++++++++++++++++++++Executing Test cases for Incremental Update Features +++++++++++++++++++++++")
        #prcs = "(prc_PrcId_1.json|prc_PrcId_2.json|prc_PrcId_3.json|prc_PrcId_10.json|prc_PrcId_11.json|prc_PrcId_20.json|prc_PrcId_21.json|prc_PrcId_22.json|prc_PrcId_23.json|prc_PrcId_24.json)"
        prcs = "(prc_PrcId_25.json)"
        pool = 3
        module.main('config/config.cnf', prcs, pool)


def delete_dest_dir(cls):
    if os.path.exists(cls.config.get('DIT_TEST_CASE_config', 'DEST_LOC_INCREMENTAL')):
        shutil.rmtree(cls.config.get('DIT_TEST_CASE_config', 'DEST_LOC_INCREMENTAL'))   
    
    if os.path.isfile(cls.config.get('DIT_TEST_CASE_config', 'DB_LOC_INCREMENTAL_JDBC')):
        os.remove(cls.config.get('DIT_TEST_CASE_config', 'DB_LOC_INCREMENTAL_JDBC'))  

    
    if os.path.exists(cls.config.get('DIT_TEST_CASE_config', 'DB_LOC_INCREMENTAL_WAREHOUSE')):
            shutil.rmtree(cls.config.get('DIT_TEST_CASE_config', 'DB_LOC_INCREMENTAL_WAREHOUSE'))
        
    if os.path.exists(cls.config.get('DIT_TEST_CASE_config', 'DB_LOC_INCREMENTAL_DERBY')):
            shutil.rmtree(cls.config.get('DIT_TEST_CASE_config', 'DB_LOC_INCREMENTAL_DERBY'),ignore_errors=True)  
        
 

class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        warnings.simplefilter('ignore', category=ImportWarning)
        warnings.simplefilter('ignore', category=DeprecationWarning)
        # instantiate config Parser 
        cls.config = ConfigParser()
        cls.config.read('config/config.cnf')
        os.environ["SPARK_CONF_DIR"] = cls.config.get('DIT_TEST_CASE_config', 'SPARK_CONF_DIR_INCREMENTAL')
        delete_dest_dir(cls)
        modulePath = os.path.join(os.path.abspath("../dataPrepartion"),'common_utils.py')
        print("Adding module py file ::"+modulePath+" to Spark context")
        cls.spark = pyspark.sql.SparkSession.builder.appName("Test_incremental_features").enableHiveSupport().getOrCreate()
        cls.spark.sparkContext.addPyFile(modulePath)
        execute_valid_process()



    def setUp(self):
        print("setup")     

     
    def tearDown(self):
        print("tearDown")     
    
    @classmethod
    def tearDownClass(cls):
        #cls.spark.stop()
        #delete_dest_dir(cls)
        print("tearDownClass")      

    '''
    Read from files, perform inner join, filter records, and then add an extra column with some default/constant value or SQL function.
    '''
    #@unittest.skip("demonstrating skipping")    
    def test_PrcId_1(self):
        print("Validating test result of PrcId_1")
        #observedDF = self.spark.read.json(self.config.get('DIT_TEST_CASE_config', 'DEST_LOC_CSV').strip()+"/DestId_1_json/json/")
        #obsCount=observedDF.count()
        #filteredCount=observedDF.filter("cat_dpt_id = '2_X_Y_Z' and dept_name = 'Fitness' ").count()
        #print("The count of records at destination location is :: "+str(obsCount))
        #print("The count of filtered records is :: "+str(filteredCount))
        #self.assertEqual(obsCount, filteredCount)




   


if __name__ == '__main__':
    unittest.main()