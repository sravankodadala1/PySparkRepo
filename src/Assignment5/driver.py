from pyspark.sql import SparkSession
from src.Assignment5.util import *


spark = SparkSession.builder.appName("CreateDataFrames").getOrCreate()

# Define schemas for DataFrames dynamically
employee_schema = define_employee_schema()
department_schema = define_department_schema()
country_schema = define_country_schema()

employee_df, department_df, country_df = create_dataframes(spark, employee_schema, department_schema, country_schema)

show_dataframes(employee_df, department_df, country_df)

avg_salary_department = calculate_avg_salary(employee_df)
avg_salary_department.show()

employees_start_with_m = find_employees_start_with_m(employee_df)
employees_start_with_m.show()

employee_df = add_bonus_column(employee_df)
employee_df.show()

employee_df = reorder_columns(employee_df)
employee_df.show()

inner_join, left_join, right_join = perform_joins(employee_df, department_df)
inner_join.show()
left_join.show()
right_join.show()

new_employee_df = derive_new_dataframe(employee_df, country_df)
new_employee_df.show()

final_df = convert_columns_and_add_load_date(new_employee_df)
final_df.show()

create_external_tables(final_df)