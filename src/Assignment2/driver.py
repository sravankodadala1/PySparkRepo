import logging
from pyspark.sql import SparkSession
from src.Assignment2.util import *

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)
logger = logging.getLogger()

spark = SparkSession.builder.appName("CreditCardProcessing").getOrCreate()


data = [("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)]
credit_card_df = create_credit_card_dataframe(spark, data)
logger.info("Number of partitions before manipulation: %d", credit_card_df.rdd.getNumPartitions())

credit_card_df = increase_partitions(credit_card_df, 5)
logger.info("Number of partitions after increasing: %d", credit_card_df.rdd.getNumPartitions())

credit_card_df = decrease_partitions(credit_card_df, 1)
logger.info("Number of partitions after decreasing: %d", credit_card_df.rdd.getNumPartitions())

mask_credit_card = mask_credit_card_udf()
credit_card_df = credit_card_df.withColumn("masked_card_number", mask_credit_card("card_number"))
credit_card_df.show(truncate=False)


