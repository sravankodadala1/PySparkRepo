from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf
import logging


def read_csv(spark, input_path):
    schema = StructType([
        StructField("data", StringType(), True)
    ])
    return spark.read.csv(input_path, header=False, schema=schema)

def mask_card_number(data):
    if len(data) >= 4:
        return "*" * (len(data) - 4) + data[-4:]
    else:
        return data

def apply_mask_udf(df):
    mask_card_udf = udf(mask_card_number, StringType())
    return df.withColumn("masked_card_number", mask_card_udf("data"))

def increase_partitions(df, num_partitions):
    return df.repartition(num_partitions)

def reduce_partitions(df, num_partitions):
    return df.coalesce(num_partitions)

def log_partitions(df, message):
    logger = logging.getLogger(__name__)
    logger.info(f"{message}: {df.rdd.getNumPartitions()}")
