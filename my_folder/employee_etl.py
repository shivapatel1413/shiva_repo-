from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType


# Current timestamp for file pattern and load_ts column
current_date = datetime.now()
date_pattern = current_date.strftime('%Y_%m_%d')
path = f'gs://sample_etl_bucket1234/source_files/hero_{date_pattern}.txt'


schema = StructType([
    StructField("EMPLOYEE_ID", IntegerType(), True),
    StructField("FIRST_NAME", StringType(), True),
    StructField("LAST_NAME", StringType(), True),
    StructField("EMAIL", StringType(), True),
    StructField("PHONE_NUMBER", StringType(), True),
    StructField("HIRE_DATE", DateType(), True),
    StructField("JOB_ID", StringType(), True),
    StructField("SALARY", DoubleType(), True),
    StructField("COMMISSION_PCT", DoubleType(), True),
    StructField("MANAGER_ID", IntegerType(), True),
    StructField("DEPARTMENT_ID", IntegerType(), True),
    StructField("FULL_NAME", StringType(), True),
    StructField("LOAD_TS", TimestampType(), True),
])

#sample_etl_bucket1234/source_files/hero_2025_07_14.txt
# Create Spark session
spark = SparkSession.builder \
    .appName("employee") \
    .getOrCreate()

# Read the CSV file
df_txt = spark.read.option('delimiter', ',').option('header', True).schema(schema).csv(path)

# Select first and last names
df_txt.select('FIRST_NAME', 'LAST_NAME').show()

# Create full name and load timestamp columns
df_txt_2 = df_txt.withColumn(
    'FULL_NAME', concat_ws(" ", col('FIRST_NAME'), col('LAST_NAME'))
).withColumn(
    'LOAD_TS', lit(current_date.strftime('%Y-%m-%d %H:%M:%S'))
)

# Drop duplicates
df_txt_2 = df_txt_2.dropDuplicates()

# Optional: Show count
print(f"Total records after deduplication: {df_txt_2.count()}")

# Define BigQuery table ID
table_id = 'etl-project-463010.krishna.employee'

# Write to BigQuery
df_txt_2.write \
    .format('bigquery') \
    .option('table', table_id) \
    .option('temporaryGcsBucket', 'sample_etl_bucket1234') \
    .mode('overwrite') \
    .save()
