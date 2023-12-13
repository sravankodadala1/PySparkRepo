import unittest
from pyspark.sql import SparkSession
from src.Assignment2.util import *

class TestCreditCardProcessing(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("unittest").getOrCreate()
        self.test_data = [("1111222233334444",),
                          ("5555666677778888",),
                          ("9999000011112222",),
                          ("1234567812345678",),
                          ("9876543210987654",)]

    def tearDown(self):
        self.spark.stop()

    def test_create_credit_card_dataframe(self):
        df = create_credit_card_dataframe(self.spark, self.test_data)
        self.assertEqual(df.count(), len(self.test_data))

    def test_increase_partitions(self):
        df = create_credit_card_dataframe(self.spark, self.test_data)
        df = increase_partitions(df, 5)
        self.assertEqual(df.rdd.getNumPartitions(), 5)

    def test_mask_credit_card_udf(self):
        mask_udf = mask_credit_card_udf()
        masked_number = mask_udf("1234567812345678")
        self.assertEqual(masked_number, "************5678")

if __name__ == '__main__':
    unittest.main()
