# -*- coding: utf-8 -*-
import sys
sys.path.append('../') 
import os
import unittest
import sqlite3
import warnings
import importlib
from configparser import ConfigParser

try:
    import pyspark
except:
    import findspark
    findspark.init()   

# instantiate config Parser
config = ConfigParser()
config.read('config\\config.cnf')

def execute_valid_process():
        module = importlib.import_module('dataPrepartion.dataIngestion')
        print(module)        
        prcs = "prc_PrcId_[4-6].json"
        pool = 3
        module.main('config\\config.cnf', prcs, pool)
        
def create_test_db():
    
    """
    Setup a temporary database
    """
    if os.path.isfile(config.get('DIT_TEST_CASE_config', 'DB_LOC_JDBC')):
        os.remove(config.get('DIT_TEST_CASE_config', 'DB_LOC_JDBC'))
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
        cls.conn=create_test_db()
        #TestFiles\\TestCsvToCsv\\destLoc\\
        execute_valid_process()

        cls.spark = pyspark.sql.SparkSession.builder.appName("Test_Jdbc_To_Jdbc").enableHiveSupport().getOrCreate()
  
    
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
        print("tearDownClass")      

    '''
    Read from files, perform inner join, filter records, and then add an extra column with some default/constant value or SQL function.
    '''
   
    def test_PrcId_4(self):
        print("Validating test result of PrcId_4")
        cursor = self.conn.cursor()     
        resultSet=cursor.execute("select job from Dest_4 where deptName='ACCOUNTING' and salary=5000").fetchall()
        cursor.close()
        self.assertEqual("PRESIDENT", resultSet[0][0])



    '''
    Read from a file, filter the data, transform data of one of the columns using SQL function, save the output in compressed file format
    '''
    @unittest.skip("demonstrating skipping")      
    def test_PrcId_5(self):
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
    def test_PrcId_6(self):
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