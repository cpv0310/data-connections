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

df.dtypes
  
df.to_csv('data/atlantic-updated.csv')
  
!hdfs dfs -cp data/atlantic-updated.csv s3a://go01-demo/user/chrisv/hurricanes/

#!hdfs dfs -ls  s3a://go01-demo/user/chrisv/hurricanes/

EXAMPLE_SQL_QUERY = "create external table default.foo (a int, b string) ROW FORMAT DELIMITED fields terminated by ',' STORED AS TEXTFILE LOCATION 's3a://eng-ml-lr-prod-env-aws/foo/'"

