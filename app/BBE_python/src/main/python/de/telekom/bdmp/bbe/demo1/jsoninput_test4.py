from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
from pyspark.sql.types import *
from datetime import datetime
# import pyspark.sql.functions as F

ts = datetime.now().strftime('%Y%m%d%H%M')

#coment1

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

# filter , 1 record
df3 = df1.filter((df1['messagetype'] == 'DigiOSS - FibreOnLocation') & (df1['acl_id'] == '100053607'))
df3.show()

#  read column 'jsonstruct' as JSON
#df_jsonschema = spark.read.json(df3.rdd.map(lambda row: row.jsonstruct))
#df_jsonschema.printSchema()

#  get schema from json column 'jsonstruct'
jsonschema_FoL = spark.read.json(df3.rdd.map(lambda row: row.jsonstruct)).schema

# parse json column
#Returns a new DataFrame by adding a column or replacing the existing column that has the same name.
#The column expression must be an expression over this DataFrame; attempting to add a column from some other dataframe will raise an error.

#df4jsn = df3.withColumn('json_data', from_json(col('jsonstruct'), jsonschema_FoL))


df4jsn = df3.withColumn('json_data', from_json(col('jsonstruct'), jsonschema_FoL))\
    .select(
    col('acl_id'),
	col('acl_dop'),
	col('acl_dop_yyyymm'),
    col('acl_loadnumber'),
	col('messagetype'),
	col('messageversion'),
    col('json_data.creationDate'),
    col('json_data.modificationDate'),
    col('json_data.klsId'),
    col('json_data.areaType')
)
    # do not use show: .show()

#insert dataframe into table
df4jsn.write.insertInto( 'db_d170_bbe_in_iws.il_tmagic_fiberOnLocation_demo0_mt', overwrite=False)
#df4jsn.show()


# test, read and show data from table
df_FoL_demo0 = spark.table('db_d170_bbe_in_iws.il_tmagic_fiberOnLocation_demo0_mt')
df_FoL_demo0.show()

