import unittest
from pyspark.sql import SparkSession
from src.Assignment1.uitl import *

class TestPurchaseAnalysisFunctions(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("test_purchase_analysis").getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_find_customers_only_product_A(self):
        data = [(1, "A"), (2, "B"), (3, "A"), (3, "B"), (4, "A")]
        schema = ["customer", "product_model"]
        test_df = self.spark.createDataFrame(data, schema=schema)
        result = find_customers_only_product_A(test_df)
        expected = [3, 4]
        self.assertEqual(result, expected)

    def test_find_customers_upgraded_B_to_E(self):
        data = [(1, "B"), (2, "B"), (3, "E"), (4, "B")]
        schema = ["customer", "product_model"]
        test_df = self.spark.createDataFrame(data, schema=schema)
        result = find_customers_upgraded_B_to_E(test_df)
        expected = [3]
        self.assertEqual(result, expected)

    def test_find_customers_all_models(self):
        data = [(1, "A"), (1, "B"), (1, "C"), (1, "D"), (1, "E")]
        schema = ["customer", "product_model"]
        test_df = self.spark.createDataFrame(data, schema=schema)
        result = find_customers_all_models(test_df)
        expected = [1]
        self.assertEqual(result, expected)

if __name__ == '__main__':
    unittest.main()
