import unittest
import logging
import sys
import importlib
import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import *
# from pyspark.sql.types import StructType, StructField
# from pyspark.sql.types import DoubleType, IntegerType, StringType

class PySparkTest(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder.master("local[*]").appName("dataIngestion")
        .enableHiveSupport().getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

from operator import add
class SimpleTest(PySparkTest):

    def testDataIngestion(self):
        #os.system('python dataIngestionTool.dataPrepartion.dataIngestion.py')
        module = importlib.import_module('dataIngestionTool.dataPrepartion.dataIngestion')
        print(module)
        prcs = "prc_PrcId_[0-9].json"
        pool = 3
        # module.main(args.config, args.prcs, args.pool)
        module.main('C:\\Users\\sk250102\\Documents\\Teradata\\DIT\\DataIngestionTool\\config\\config.cnf', prcs, pool)

    def testCsvToJson(self):
        schema = StructType([
            StructField("category_id", IntegerType()),
            StructField("category_department_id", DoubleType()),
            StructField("category_name", StringType())
        ])

        actualDF = self.spark.read.schema(schema).option("header", "true").csv(
            "C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\test_dataIngestionTool\\srcLoc\\category.csv")

        print(actualDF.count())

        observedDF = self.spark.read.json(
            "C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\test_dataIngestionTool\\trgLoc\\DestId_1_json\\json\\")

        print(observedDF.count())
        print("CSV to Json")
        self.assertEqual(actualDF.count(), observedDF.count())

    def testCsvToParquet(self):
        schema = StructType([
            StructField("category_id", IntegerType()),
            StructField("category_department_id", DoubleType()),
            StructField("category_name", StringType())
        ])

        actualDF = self.spark.read.schema(schema).option("header", "true").csv(
            "C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\test_dataIngestionTool\\srcLoc\\category.csv")

        print(actualDF.count())

        observedDF = self.spark.read.parquet(
            "C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\test_dataIngestionTool\\trgLoc\\DestId_2_parquet\\parquet\\")

        print(observedDF.count())
        print("CSV to Parquet")
        self.assertEqual(actualDF.count(), observedDF.count())

    def testCsvToCsv(self):
        schema = StructType([
            StructField("category_id", IntegerType()),
            StructField("category_department_id", DoubleType()),
            StructField("category_name", StringType())
        ])

        actualDF = self.spark.read.schema(schema).option("header", "false").csv(
        "C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\test_dataIngestionTool\\srcLoc\\category.csv")

        print(actualDF.count())

        schema1 = StructType([
            StructField("cat_id", IntegerType()),
            StructField("cat_dpt_id", IntegerType()),
            StructField("cat_name", StringType())
        ])
        observedDF = self.spark.read.schema(schema1).option("header", "true").csv(
            "C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\test_dataIngestionTool\\trgLoc\\DestId_3_csv\\csv\\")

        print(observedDF.count())
        print("CSV to CSV")
        self.assertEqual(actualDF.count(), observedDF.count())

    def testCsvToOrc(self):
        schema = StructType([
            StructField("category_id", IntegerType()),
            StructField("category_department_id", DoubleType()),
            StructField("category_name", StringType())
        ])

        actualDF = self.spark.read.schema(schema).option("header", "true").csv(
            "C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\test_dataIngestionTool\\srcLoc\\category.csv")

        print(actualDF.count())

        observedDF = self.spark.read.orc(
            "C:\\Users\\aj250046\\Documents\\DIT2\\DataIngestionTool\\test_dataIngestionTool\\trgLoc\\DestId_4_orc\\orc\\")

        print(observedDF.count())
        print("CSV to orc")
        self.assertEqual(actualDF.count(), observedDF.count())

if __name__ == "__main__":
    print("start of main")
    unittest.main()



