from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def create_credit_card_dataframe(spark, data, column_name="card_number"):
    return spark.createDataFrame(data, [column_name])

def increase_partitions(dataframe, num_partitions):
    return dataframe.repartition(num_partitions)

def decrease_partitions(dataframe, num_partitions):
    return dataframe.coalesce(num_partitions)

def mask_credit_card_udf():
    @udf(StringType())
    def mask_credit_card(card_number):
        masked_number = "*" * (len(card_number) - 4) + card_number[-4:]
        return masked_number

    return mask_credit_card
