import unittest
from pyspark.sql import SparkSession
from src.Assignment3.util import *

class TestUserLoginDetails(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("unittest").getOrCreate()
        self.data = [
            (1, 101, 'login', '2023-09-05 08:30:00'),
            (2, 102, 'click', '2023-09-06 12:45:00'),
            (3, 101, 'click', '2023-09-07 14:15:00'),
            (4, 103, 'login', '2023-09-08 09:00:00'),
            (5, 102, 'logout', '2023-09-09 17:30:00'),
            (6, 101, 'click', '2023-09-10 11:20:00'),
            (7, 103, 'click', '2023-09-11 10:15:00'),
            (8, 102, 'click', '2023-09-12 13:10:00')
        ]
        self.columns = ["log_id", "user_id", "user_activity", "time_stamp"]
        self.df = create_dataframe(self.spark, self.data, self.columns)

    def tearDown(self):
        self.spark.stop()

    def test_calculate_total_actions(self):
        result_df = calculate_total_actions(self.df)
        self.assertEqual(result_df.count(), 3)

    def test_convert_timestamp_to_login_date(self):
        result_df = convert_timestamp_to_login_date(self.df)
        self.assertTrue("login_date" in result_df.columns) 
        self.assertTrue(result_df.select("login_date").count() == len(self.data))

    def test_write_to_csv(self):
        output_path = "C:/Users/sravan/Documents/test_output.csv"
        write_to_csv(self.df, output_path)
        csv_exists = self.df.sparkSession.read.option("header", "true").csv(output_path).count() > 0
        self.assertTrue(csv_exists)

    def test_write_to_managed_table(self):
        database_name = "test_db"
        table_name = "test_table"
        write_to_managed_table(self.df, database_name, table_name)
        table_exists = self.spark.catalog.tableExists(f"{database_name}.{table_name}")
        self.assertTrue(table_exists)

if __name__ == '__main__':
    unittest.main()
