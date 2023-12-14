from pyspark import SparkSession
from src.Assignment3.util import *

spark=SparkSession.builder.appName("Userlogindetails").getOrCreate()

data = [
    (1, 101, 'login', '2023-09-05 08:30:00'), 
    (2, 102, 'click', '2023-09-06 12:45:00'),
    (3, 101, 'click', '2023-09-07 14:15:00'),
    (4, 103, 'login', '2023-09-08 09:00:00'),
    (5, 102, 'logout', '2023-09-09 17:30:00'),
    (6, 101, 'click', '2023-09-10 11:20:00'),
    (7, 103, 'click', '2023-09-11 10:15:00'),
    (8, 102, 'click', '2023-09-12 13:10:00')
]

columns = ["log_id", "user_id", "user_activity", "time_stamp"]
df = create_dataframe(spark, data, columns)

#Calculate the total number of actions performed by each user
result_df = calculate_total_actions(df)
result_df.show()

# Convert the timestamp column to login_date with yyyy-MM-dd format
df = convert_timestamp_to_login_date(df)

# Write the data frame as a CSV file
csv_output_path = "C:/Users/sravan/Documents/Book1.csv"
write_to_csv(df, csv_output_path)

#Write it as a managed table with Database name as user and table name as login_details with overwrite mode
database_name = "user"
table_name = "login_details"
write_to_managed_table(df, database_name, table_name)

