#sample read json file and create a table in datalake with it
import cml.data_v1 as cmldata


CONNECTION_NAME = "go01-aw-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

spark_dataframe = spark.read.json("data/people.json")
# Displays the content of the DataFrame to stdout
spark_dataframe.show()
pandas_dataframe = df.toPandas()
print (pandas_dataframe)

spark_dataframe.write \
  .format("parquet") \
  .mode("overwrite") \
  .saveAsTable("default.people")
  
spark.sql('select * from default.people').show()

#sample of writing json file directly converted to parquet format into s3
spark_dataframe.write.parquet("s3a://go01-demo/user/charu/people.parquet")

# Still need to do create external table on it to query it through impala
# So, now need impala connection
