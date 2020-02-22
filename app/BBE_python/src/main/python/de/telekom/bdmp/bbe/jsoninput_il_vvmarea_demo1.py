from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col
from pyspark.sql.types import *
from datetime import datetime
# import pyspark.sql.functions as F

ts = datetime.now().strftime('%Y%m%d%H%M')



spark = SparkSession \
    .builder \
    .appName("miro_bbe_session_{ts}") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .getOrCreate()
# read data from table to DataFrame "df1",  db_d170_bbe_in_iws.il_tmagic_json_test_vvmArea_ET
df1 = spark.sql("select * from db_d170_bbe_in_iws.il_tmagic_json_test_vvmArea_ET")

# filter "vvm" only messages
df3 = df1.filter((df1['messagetype'] == 'DigiOSS - vvmArea')  & (df1['Messageversion'] == '1'))

#  get schema from json-column 'jsonstruct'
jsonschema_FoL = spark.read.json(df3.rdd.map(lambda row: row.jsonstruct)).schema

# new dataframe , select columns for target table , using values from json....
df4jsn = df3.withColumn('json_data', from_json(col('jsonstruct'), jsonschema_FoL))\
    .select(
    col('acl_id'),
    to_timestamp(col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
    col('json_data.name'),
    col('json_data.number'),
    from_unixtime(col('json_data.creationDate')[0:10]).alias('creationDate_ISO') ,
    from_unixtime(col('json_data.modificationDate')[0:10]).alias('modificationDate_ISO'),
    col('json_data.areaType'),
    from_unixtime(col('json_data.rolloutDate')[0:10]).alias('rolloutDate_ISO'),
    col('json_data.areaStatus'),
    col('json_data.plannedArea'),
    from_unixtime(col('json_data.plannedFrom')[0:10]).alias('plannedFrom_ISO'),
    from_unixtime(col('json_data.plannedTo')[0:10]).alias('plannedTo_ISO'),
    )


# db_d170_bbe_in_iws.il_tmagic_fiberOnLocation_demo2_mt
#insert dataframe into table   db_d170_bbe_core_iws.cl_tmagic_vvm_area_mt
df4jsn.write.insertInto('db_d170_bbe_core_iws.cl_tmagic_vvm_area_mt', overwrite=True)

# test , just for demonstration, read and show data from target table
df_FoL_demo0 = spark.table('db_d170_bbe_core_iws.cl_tmagic_vvm_area_mt')
df_FoL_demo0.show()

