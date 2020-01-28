from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
from datetime import datetime

ts = datetime.now().strftime('%Y%m%d%H%M')

spark = SparkSession \
    .builder \
    .appName("miro_bbe_session_{ts}") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .getOrCreate()

df1 = spark.sql("select * from db_d170_bbe_iws_pwr.IL_TMagic_jsoninput_ET")

#df2 = df1.filter((df1['messagetype'] == 'DigiOSS - FibreOnLocation') & (df1['acl_id'] == '100053607'))  \
#    .select('jsonstruct').collect()
#print(df2)

df3 = df1.filter((df1['messagetype'] == 'DigiOSS - FibreOnLocation') & (df1['acl_id'] == '100053607'))
df3.show()

#df_jsonschema = spark.read.json(df3.rdd.map(lambda row: row.jsonstruct))
#df_jsonschema.printSchema()

jsonschema0 = spark.read.json(df3.rdd.map(lambda row: row.jsonstruct)).schema

df4 = df3.withColumn('jsonstruct', from_json(col('jsonstruct'), jsonschema0)).select('jsonstruct.*')
df4.show()



