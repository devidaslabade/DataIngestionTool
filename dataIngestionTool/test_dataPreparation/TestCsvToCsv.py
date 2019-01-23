# -*- coding: utf-8 -*-
import pyspark
import unittest
import warnings
import importlib
import shutil
from configparser import ConfigParser
import os
import sys
sys.path.append('../')

# instantiate config Parser
config = ConfigParser()

def execute_valid_process():
        module = importlib.import_module('dataPrepartion.dataIngestion')
        print(module)
        prcs = "prc_PrcId_[1-3].json"
        pool = 3
        module.main('config\\config.cnf', prcs, pool)
        
def delete_dest_dir():
    shutil.rmtree('TestFiles\\TestCsvToCsv\\destLoc\\', ignore_errors=True)

class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        warnings.simplefilter('ignore', category=ImportWarning)
        warnings.simplefilter('ignore', category=DeprecationWarning)
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
        print("tearDownClass")      

    '''
    Read from files, perform inner join, filter records, and then add an extra column with some default/constant value or SQL function.
    '''
    def test_PrcId_1(self):
        print("Validating test result of PrcId_1")
        observedDF = self.spark.read.json("TestFiles\\TestCsvToCsv\\destLoc\\DestId_1_json\\json\\")
        obsCount=observedDF.count()
        filteredCount=observedDF.filter("cat_dpt_id = '2_X_Y_Z' and dept_name = 'Fitness' ").count()
        print("The count of records at destination location is :: "+str(obsCount))
        print("The count of filtered records is :: "+str(filteredCount))
        self.assertEqual(obsCount, filteredCount)




    '''
    Read from a file, filter the data, transform data of one of the columns using SQL function, save the output in compressed file format
    '''
    def test_PrcId_2(self):
        print("Validating test result of PrcId_2")
        isValid=False
        destDir="TestFiles\\TestCsvToCsv\\destLoc\\DestId_2_json\\json\\"
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

    def test_PrcId_3(self):
        print("Validating test result of PrcId_3")
        observedDF = self.spark.read.json("TestFiles\\TestCsvToCsv\\destLoc\\DestId_3_json\\json\\")
        obsCount=observedDF.show()
        filteredCount=observedDF.filter("category_department_id = 8 and cnt_cat = 10 ").count()
        #print("The count of records at destination location is :: "+str(obsCount))
        print("The count of filtered records is :: "+str(filteredCount))
        self.assertEqual(1, filteredCount)



        
        
    @unittest.skip("demonstrating skipping")    
    def test_02(self):
        self.assertFalse(False)
     
        
          







if __name__ == '__main__':
    unittest.main()