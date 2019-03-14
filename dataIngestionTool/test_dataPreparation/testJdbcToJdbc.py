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
        print("+++++++++++++++++++++Executing Test cases with JDBC supported DB  +++++++++++++++++++++++")   
        prcs = "(prc_PrcId_4.json|prc_PrcId_5.json|prc_PrcId_6.json|prc_PrcId_14.json|prc_PrcId_15.json)"
        pool = 3
        module.main('config/config.cnf', prcs, pool)
        
def delete_dest_dir():
    if os.path.exists(config.get('DIT_TEST_CASE_config', 'DEST_LOC_JDBC')):
        shutil.rmtree(config.get('DIT_TEST_CASE_config', 'DEST_LOC_JDBC'))   
        
    if os.path.isfile(config.get('DIT_TEST_CASE_config', 'DB_LOC_JDBC')):
       os.remove(config.get('DIT_TEST_CASE_config', 'DB_LOC_JDBC'))       
    
    if os.path.exists(config.get('DIT_TEST_CASE_config', 'DB_LOC_JDBC_WAREHOUSE')):
        shutil.rmtree(config.get('DIT_TEST_CASE_config', 'DB_LOC_JDBC_WAREHOUSE'))
        
    if os.path.exists(config.get('DIT_TEST_CASE_config', 'DB_LOC_JDBC_DERBY')):
        shutil.rmtree(config.get('DIT_TEST_CASE_config', 'DB_LOC_JDBC_DERBY'),ignore_errors=True)         
        
def create_test_db():
    
    """
    Setup a temporary database
    """
    conn = sqlite3.connect(config.get('DIT_TEST_CASE_config', 'DB_LOC_JDBC'))
    cursor = conn.cursor()
    # create a table
    cursor.execute("""CREATE TABLE employee (empId text,empName text,job text,manager text,hiredate text,salary text,comm text,deptno text)""")
    cursor.execute("""CREATE TABLE department (deptno text,dname text,loc text)""")
    # insert some data
    cursor.execute("INSERT INTO department VALUES ('10', 'ACCOUNTING', 'NEW YORK')")
    cursor.execute("INSERT INTO department VALUES ('20', 'RESEARCH', 'BOSTON')")
    cursor.execute("INSERT INTO department VALUES ('30', 'SALES', 'CHICAGO')")
    cursor.execute("INSERT INTO department VALUES ('40', 'OPERATIONS', 'BOSTON')")
    cursor.execute("INSERT INTO department VALUES ('50', 'ADMIN', 'CHICAGO')")
    # save data to database
    conn.commit()
    # insert multiple records using the more secure "?" method
    empDetails = [('7839','KING','PRESIDENT','null','17-11-1981','5000','null','10'),
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
    cursor.executemany("INSERT INTO employee VALUES (?,?,?,?,?,?,?,?)", empDetails)

    conn.commit()
    return conn

class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        warnings.simplefilter('ignore', category=ImportWarning)
        warnings.simplefilter('ignore', category=DeprecationWarning)
        os.environ["SPARK_CONF_DIR"] = config.get('DIT_TEST_CASE_config', 'SPARK_CONF_DIR_JDBC')
        delete_dest_dir()
        cls.conn=create_test_db()
        #TestFiles/TestCsvToCsv/destLoc/
        cls.spark = pyspark.sql.SparkSession.builder.appName("Test_Jdbc_To_Jdbc").enableHiveSupport().getOrCreate()
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
        cls.conn.close()
        #os.remove(config.get('DIT_TEST_CASE_config', 'DB_LOC_JDBC'))
        cls.spark.stop()
        delete_dest_dir()
        print("tearDownClass")      

    '''
    Read from files, perform inner join, filter records, and then add an extra column with some default/constant value or SQL function.
    '''
    #@unittest.skip("demonstrating skipping")
    def test_PrcId_4(self):
        print("Validating test result of PrcId_4")
        cursor = self.conn.cursor()     
        resultSet=cursor.execute("select job from Dest_4 where deptName='ACCOUNTING' and salary=5000").fetchall()
        cursor.close()
        self.assertEqual("PRESIDENT", resultSet[0][0])



    '''
    Read from a file, filter the data, transform data of one of the columns using SQL function, save the output in compressed file format
    '''
    #@unittest.skip("demonstrating skipping")     
    def test_PrcId_5(self):
        print("Validating test result of PrcId_5")        
        cursor = self.conn.cursor()     
        resultSet=cursor.execute('select salary from Dest_5 where departmentNo=20 and employeeName="ADAMS"').fetchall()        
        cursor.close()
        print(resultSet)
        self.assertEqual('1100', resultSet[0][0])



    '''
    Read from files, perform inner join, filter records, and then add an extra column with some default/constant value or SQL function.
    '''
    #@unittest.skip("demonstrating skipping")
    def test_PrcId_6(self):
        print("Validating test result of PrcId_6")
        cursor = self.conn.cursor()     
        resultSet=cursor.execute('select empCount from Dest_6 where loc="BOSTON"').fetchall()        
        cursor.close()
        print(resultSet)
        self.assertEqual(5, resultSet[0][0])



        
        
    #@unittest.skip("demonstrating skipping")    
    def test_PrcId_14(self):
        print("Validating test result of PrcId_14")
        # Read from Hive
        df_load = self.spark.sql('select employeeName from fin_tab_dest14 where deptName = "ACCOUNTING" and job = "CLERK"')
        #df_load.show()
        retVal=df_load.collect()
        #print(retVal[0].job)
        self.assertEqual("MILLER", retVal[0]['employeeName'])
     
    #@unittest.skip("demonstrating skipping")         
    def test_PrcId_15(self):
        print("Validating test result of PrcId_15")
        observedDF = self.spark.read.orc(config.get('DIT_TEST_CASE_config', 'DEST_LOC_JDBC').strip()+"/DestId_15_orc/orc/")
        #obsCount=observedDF.show()
        filteredData=observedDF.filter('deptName = "ACCOUNTING" and job = "MANAGER"').select("employeeName").collect()
        #print("The count of records at destination location is :: "+str(obsCount))
        #print("The count of filtered records is :: "+str(filteredCount))
        self.assertEqual('CLARK', filteredData[0]['employeeName'])          







if __name__ == '__main__':
    unittest.main()