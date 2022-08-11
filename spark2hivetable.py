import pandas as pd
import cml.data_v1 as cmldata

CONNECTION_NAME = "go01-aw-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the CSV and infer schema
df=spark.read.options(header='True',inferSchema='True').csv("data/atlantic-updated.csv")

# Write the data to a hive table
df.write \
  .format("parquet") \
  .mode("overwrite") \
  .saveAsTable("default.hurricane2")


## Sample Usage to get spark data frame
EXAMPLE_SQL_QUERY = "select * from default.hurricane2 where name='ALICIA'"
df2 = spark.sql(EXAMPLE_SQL_QUERY)

## Sample to get to pandas data frame
pandasDF= df2.toPandas()





      




