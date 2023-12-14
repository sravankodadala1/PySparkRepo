from pyspark.sql import SparkSession
from pyspark.sql.functions import count, date_format



def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()
 
def create_dataframe(spark, data, columns):
    return spark.createDataFrame(data, columns)

def calculate_total_actions(dataframe):
    return dataframe.groupBy("user_id").agg(count("user_activity").alias("total_actions"))

def convert_timestamp_to_login_date(dataframe):
    return dataframe.withColumn("login_date", date_format("time_stamp", "yyyy-MM-dd"))

def write_to_csv(dataframe, output_path):
    dataframe.write.mode("overwrite").option("header", "true").csv(output_path)

def write_to_managed_table(dataframe, database_name, table_name):
    dataframe.write.mode("overwrite").format("parquet").saveAsTable(f"{database_name}.{table_name}")
