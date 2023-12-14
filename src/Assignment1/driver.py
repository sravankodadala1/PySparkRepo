from pyspark.sql import SparkSession
from src.Assignment1.util import *

spark = SparkSession.builder.appName("Purchase_analysis").getOrCreate()

purchase_data_df, product_data_df = create_dataframes(spark)

# customers who have bought only product A
customers_only_product_A = find_customers_only_product_A(purchase_data_df) 
customers_only_product_A.show()

# customers who upgraded from product B to product E
customers_upgraded_B_to_E = find_customers_upgraded_B_to_E(purchase_data_df)
customers_upgraded_B_to_E.show()

# ustomers who have bought all models in the new Product Data
customers_all_models = find_customers_all_models(purchase_data_df)
customers_all_models.show()
