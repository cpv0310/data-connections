import pandas as pd
import cml.data_v1 as cmldata

df=pd.read_csv("data/atlantic.csv")

##Clean the data

df.columns = [c.replace(' ', '_') for c in df.columns]


df["Time"] = df["Time"].astype("object")
time_replace = [str(x) for x in df["Time"].unique()]
for i, txt in enumerate(time_replace):
    time_replace[i] = txt.rjust(4, "0")
    time_replace[i] = f"{time_replace[i][0:2]}:{time_replace[i][2:4]}:00"
for old, new in zip(df["Time"].unique(), time_replace):
    df.loc[df["Time"]==old, "Time"] = new

df["Date"] = df["Date"].astype("object")
for i, date_str in enumerate(df["Date"].unique()):
    df.loc[df["Date"]==date_str, "Date"] = f"{str(date_str)[0:4]}-{str(date_str)[4:6]}-{str(date_str)[6:]}"

df["Datetime"] = df["Date"]+" "+df["Time"]
df["Datetime"] = pd.to_datetime(df["Datetime"])
df.drop(columns=["Date", "Time"], inplace=True)
df.sort_values(by=["Datetime"], inplace=True)

df["Name"] = df["Name"].str.strip()
df["Status"] = df["Status"].str.strip()
df["Event"] = df["Event"].str.strip()


###  Pull data from Pandas dataframe to Spark dataframe

CONNECTION_NAME = "go01-aw-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

sparkDF=spark.createDataFrame(df[(df['Datetime']>'1901-01-01 00:00:00')]) 

### Write Spark DF to Hive Table

sparkDF.write \
        .format("parquet") \
        .mode("overwrite") \
        .saveAsTable("default.hurricane")


### Read Data with Impala

CONNECTION_NAME = "default-impala"
conn = cmldata.get_connection(CONNECTION_NAME)

## Sample Usage to get pandas data frame
EXAMPLE_SQL_QUERY = "select * from default.hurricane"
pandas_dataframe = conn.get_pandas_dataframe(EXAMPLE_SQL_QUERY)
print(pandas_dataframe)

