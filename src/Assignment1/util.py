from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import collect_set, array_contains, col


def create_dataframes(spark):
    purchase_data = [
        (1, "A"), (1, "B"), (2, "A"), (2, "B"), (3, "A"),
        (3, "B"), (1, "C"), (1, "D"), (1, "E"), (3, "E"), (4, "A")
    ] 

    product_data = [("A",), ("B",), ("C",), ("D",), ("E",)]

    purchase_schema = StructType([
        StructField("customer", IntegerType(), True),
        StructField("product_model", StringType(), True)
    ])

    product_schema = StructType([
        StructField("product_model", StringType(), True)
    ])

    purchase_data_df = spark.createDataFrame(purchase_data, schema=purchase_schema)
    product_data_df = spark.createDataFrame(product_data, schema=product_schema)

    return purchase_data_df, product_data_df

def find_customers_only_product_A(purchase_data_df):
    return purchase_data_df.groupBy("customer") \
        .agg({"product_model": "collect_set"}) \
        .filter("size(collect_set(product_model)) = 1 and collect_set(product_model)[0] = 'A'") \
        .select("customer")

def find_customers_upgraded_B_to_E(purchase_data_df):
    customers_B = purchase_data_df.filter("product_model = 'B'").select("customer")
    customers_E = purchase_data_df.filter("product_model = 'E'").select("customer")
    return customers_B.intersect(customers_E)

def find_customers_all_models(purchase_data_df):
    return purchase_data_df.groupBy("customer") \
        .agg(collect_set("product_model").alias("purchased_models")) \
        .filter(
            array_contains(col("purchased_models"), "A") &
            array_contains(col("purchased_models"), "B") &
            array_contains(col("purchased_models"), "C") &
            array_contains(col("purchased_models"), "D") &
            array_contains(col("purchased_models"), "E")
        ) \
        .select("customer")
