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
        prcs = "(prc_PrcId_7.json|prc_PrcId_8.json|prc_PrcId_9.json|prc_PrcId_12.json|prc_PrcId_13.json)"
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
        
        if os.path.isfile(config.get('DIT_TEST_CASE_config', 'DB_LOC_HIVE')):
            os.remove(config.get('DIT_TEST_CASE_config', 'DB_LOC_HIVE'))    

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
    #@unittest.skip("demonstrating skipping") 
    def test_PrcId_7(self):
        print("Validating test result of PrcId_7")
        # Read from Hive
        df_load = self.spark.sql('select job from fin_tab_dest7 where deptName="ACCOUNTING" and salary=5000')
        retVal=df_load.collect()
        #print(retVal[0].job)
        self.assertEqual("PRESIDENT", retVal[0]['job'])



    '''
    Read from a file, filter the data, transform data of one of the columns using SQL function, save the output in compressed file format
    '''
    #@unittest.skip("demonstrating skipping") 
    def test_PrcId_8(self):
        print("Validating test result of PrcId_8")        
        # Read from Hive
        df_load = self.spark.sql('select salary from fin_tab_dest8 where departmentNo=20 and employeeName="JONES"')
        #df_load.show()
        retVal=df_load.collect()
        self.assertEqual('2975', retVal[0]['salary'])



    '''
    Read from files, perform inner join, filter records, and then add an extra column with some default/constant value or SQL function.
    '''
    #@unittest.skip("demonstrating skipping")     
    def test_PrcId_9(self):
        print("Validating test result of PrcId_9")
        # Read from Hive
        df_load = self.spark.sql('select empCount from fin_tab_dest9 where loc="CHICAGO"')
        #df_load.show()
        retVal=df_load.collect()
        self.assertEqual(6, retVal[0]['empCount'])



        
        
    #@unittest.skip("demonstrating skipping")    
    def test_PrcId_12(self):
        print("Validating test result of PrcId_12")
        conn = sqlite3.connect(config.get('DIT_TEST_CASE_config', 'DB_LOC_HIVE'))
        cursor = conn.cursor()
        # Read from JDBC Source
        resultSet=cursor.execute('select employeeName from Dest_12 where deptName = "ACCOUNTING" and job = "MANAGER"').fetchall()        
        cursor.close()
        conn.close()
        #print(resultSet)
        self.assertEqual('CLARK', resultSet[0][0])
     
        
          

    #@unittest.skip("demonstrating skipping")    
    def test_PrcId_13(self):
        print("Validating test result of PrcId_13")
        observedDF = self.spark.read.json("TestFiles\\TestHiveToHive\\destLoc\\DestId_13_json\\json\\")
        #obsCount=observedDF.show()
        filteredData=observedDF.filter('deptName = "ACCOUNTING" and job = "MANAGER"').select("employeeName").collect()
        #print("The count of records at destination location is :: "+str(obsCount))
        #print("The count of filtered records is :: "+str(filteredCount))
        self.assertEqual('CLARK', filteredData[0]['employeeName'])





if __name__ == '__main__':
    unittest.main(warnings='ignore')