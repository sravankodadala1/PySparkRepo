# driver.py

from src.Assignment2.util import *

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


spark = SparkSession.builder.appName("Assignment2).getOrCreate()

input_path = "C:/Users/sravan/Documents/Book1.csv"

#Read the CSV file into a DataFrame
credit_card_df = read_csv(spark, input_path)
log_partitions(credit_card_df, "Number of partitions before processing")

# Repartition the DataFrame to increase the partition size to 5
increase_credit_card_df = increase_partitions(credit_card_df, 5)
log_partitions(increase_credit_card_df, "Number of partitions after increasing")

# Assuming the original number of partitions was stored in a variable 'original_partitions'
back_to_og_credit_card_df = reduce_partitions(increase_credit_card_df, credit_card_df.rdd.getNumPartitions())
log_partitions(back_to_og_credit_card_df, "Number of partitions after reducing")

# Save DataFrame to disk in Parquet format
credit_card_df.write.parquet("C:/Users/sravan/Documents/Book1.csv/Question2", mode="overwrite")

# Read the saved Parquet file back to in-memory as DataFrame
credit_card_df_from_disk = spark.read.parquet("C:/Users/sravan/Documents/Book1.csv/Question2")

display(credit_card_df_from_disk)

# Apply the UDF to create a new column 'masked_card_number'
credit_card_df_with_masked = apply_mask_udf(credit_card_df)
credit_card_df_with_masked.show()
