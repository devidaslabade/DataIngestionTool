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

# instantiate config Parser
config = ConfigParser()
config.read('config\\config.cnf')

def execute_valid_process():
        module = importlib.import_module('dataPrepartion.dataIngestion')
        print(module)
        prcs = "prc_PrcId_[7-7].json"
        pool = 3
        module.main('config\\config.cnf', prcs, pool)
        
def create_test_db(sparkS):
    
    """
    Setup a temporary database
    """
          
    deptSchema = StructType([StructField("deptno", StringType(), True),
                             StructField("dname", StringType(), True),
                             StructField("loc", StringType(), True)])
    deptData = [('10', 'ACCOUNTING', 'NEW YORK'),
                ('20', 'RESEARCH', 'BOSTON'),
                ('30', 'SALES', 'CHICAGO'),
                ('40', 'OPERATIONS', 'BOSTON'),
                ('50', 'ADMIN', 'CHICAGO')]
    # create a department table
    deptDF = sparkS.createDataFrame(deptData,deptSchema)
    deptDF.write.saveAsTable('department')
    '''
    fldNames=[]
    for strctFld in deptSchema:
            print(strctFld.jsonValue()['name'])
            fldNames.append(strctFld.jsonValue()['name'])
    print(','.join(fldNames))   ''' 
    # create a employee table
    empSchema = StructType([StructField("empId", StringType(), True),
                            StructField("empName", StringType(), True),
                            StructField("job", StringType(), True),
                            StructField("manager", StringType(), True),
                            StructField("hiredate", StringType(), True),
                            StructField("salary", StringType(), True),
                            StructField("comm", StringType(), True),
                            StructField("deptno", StringType(), True)]) 

    empData = [('7839','KING','PRESIDENT','null','17-11-1981','5000','null','10'),
            ('7698','BLAKE','MANAGER','7839','1-5-1981','2850','null','30'),
            ('7782','CLARK','MANAGER','7839','9-6-1981','2450','null','10'),
            ('7566','JONES','MANAGER','7839','2-4-1981','2975','null','20'),
            ('7788','SCOTT','ANALYST','7566','13-7-1987','3000','null','20'),
            ('7902','FORD','ANALYST','7566','3-12-1981','3000','null','20'),
            ('7369','SMITH','CLERK','7902','17-12-1980','800','null','20'),
            ('7499','ALLEN','SALESMAN','7698','20-2-1981','1600','300','30'),
            ('7521','WARD','SALESMAN','7698','22-2-1981','1250','500','30'),
            ('7654','MARTIN','SALESMAN','7698','28-9-1981','1250','1400','30'),
            ('7844','TURNER','SALESMAN','7698','8-9-1981','1500','0','30'),
            ('7876','ADAMS','CLERK','7788','13-7-1987','1100','null','20'),
            ('7900','JAMES','CLERK','7698','3-12-1981','950','null','30'),
            ('7934','MILLER','CLERK','7782','23-1-1982','1300','null','10')]

    # create a department table
    empDF = sparkS.createDataFrame(empData,empSchema)
    empDF.write.saveAsTable('employee')


class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        warnings.simplefilter('ignore', category=ImportWarning)
        warnings.simplefilter('ignore', category=DeprecationWarning)
        if os.path.exists(config.get('DIT_TEST_CASE_config', 'DB_LOC_HIVE_WAREHOUSE')):
            shutil.rmtree(config.get('DIT_TEST_CASE_config', 'DB_LOC_HIVE_WAREHOUSE'), ignore_errors=True)
        if os.path.exists(config.get('DIT_TEST_CASE_config', 'DB_LOC_HIVE_DERBY')):
            shutil.rmtree(config.get('DIT_TEST_CASE_config', 'DB_LOC_HIVE_DERBY'), ignore_errors=True)  

        os.environ["SPARK_CONF_DIR"] = config.get('DIT_TEST_CASE_config', 'SPARK_CONF_DIR_HIVE')
        cls.spark = pyspark.sql.SparkSession.builder.appName("Test_Hive_To_Hive")\
                    .enableHiveSupport().getOrCreate()
        create_test_db(cls.spark)   
        
        #TestFiles\\TestCsvToCsv\\destLoc\\  
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
        #cls.conn.close()
        #os.remove(config.get('DIT_TEST_CASE_config', 'DB_LOC'))
        cls.spark.stop()
        print("tearDownClass")      

    '''
    Read from files, perform inner join, filter records, and then add an extra column with some default/constant value or SQL function.
    '''
   
    def test_PrcId_7(self):
        print("Validating test result of PrcId_7")
        # Read from Hive
        df_load = self.spark.sql('SELECT * FROM fin_table')
        df_load.show()



    '''
    Read from a file, filter the data, transform data of one of the columns using SQL function, save the output in compressed file format
    '''
    @unittest.skip("demonstrating skipping")      
    def test_PrcId_8(self):
        print("Validating test result of PrcId_5")        
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
    @unittest.skip("demonstrating skipping")  
    def test_PrcId_9(self):
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