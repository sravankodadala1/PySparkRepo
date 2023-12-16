import unittest
from pyspark.sql import SparkSession
from src.Assignment2.util import *

class TestUtils(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_read_csv(self):
        # Test read_csv function
        input_path = "C:/Users/sravan/Documents/Book1.csv"
        df = read_csv(self.spark, input_path)
        self.assertTrue(df is not None)

    def test_increase_partitions(self):
        # Test increase_partitions function
        input_df = self.spark.createDataFrame([(1, "data1"), (2, "data2")], ["id", "data"])
        increased_df = increase_partitions(input_df, 5)
        self.assertEqual(increased_df.rdd.getNumPartitions(), 5)

    def test_reduce_partitions(self):
        # Test reduce_partitions function
        input_df = self.spark.createDataFrame([(1, "data1"), (2, "data2")], ["id", "data"])
        reduced_df = reduce_partitions(input_df, 1)
        self.assertEqual(reduced_df.rdd.getNumPartitions(), 1)

if __name__ == '__main__':
    unittest.main()
