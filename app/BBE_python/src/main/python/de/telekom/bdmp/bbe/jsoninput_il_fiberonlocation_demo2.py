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
# read data from table to DataFrame "df1"
df1 = spark.sql("select * from db_d170_bbe_iws_pwr.IL_TMagic_jsoninput_ET")

# filter "FoL" only messages
df3 = df1.filter((df1['messagetype'] == 'DigiOSS - FibreOnLocation')  & (df1['Messageversion'] == '1'))

#  get schema from json-column 'jsonstruct'
jsonschema_FoL = spark.read.json(df3.rdd.map(lambda row: row.jsonstruct)).schema

# new dataframe , select columns for target table , using values from json....
df4jsn = df3.withColumn('json_data', from_json(col('jsonstruct'), jsonschema_FoL))\
    .select(
    col('acl_id'),
	col('acl_dop'),
    to_timestamp(col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
	col('acl_dop_yyyymm'),
	col('messagetype'),
	col('messageversion'),
    from_unixtime(col('json_data.creationDate')[0:10]).alias('creationDate_ISO') ,
    from_unixtime(col('json_data.modificationDate')[0:10]).alias('modificationDate_ISO'),
    col('json_data.klsId'),
    col('json_data.vvmArea.number'),
    col('json_data.nvtArea.businessId'),
    col('json_data.installationStatus'),
    col('json_data.demandCarrier'),
    col('json_data.areaType'),
    col('json_data.initiative'),
    col('json_data.reasonForNoConstruction'),
    col('json_data.technology'),
    col('json_data.rolloutSponsor'),
    from_unixtime(col('json_data.installationDate')[0:10]).alias('installationDate_ISO'),
    from_unixtime(col('json_data.plannedInstallationBegin')[0:10]).alias('plannedInstallationBegin_ISO'),
    from_unixtime(col('json_data.plannedInstallationEnd')[0:10]).alias('plannedInstallationEnd_ISO'),
    col('json_data.we'),
    col('json_data.ge'),
    col('json_data.sl'))


#insert dataframe into table
df4jsn.write.insertInto('db_d170_bbe_in_iws.il_tmagic_fiberOnLocation_demo2_mt', overwrite=True)

# test , just for demonstration, read and show data from target table
df_FoL_demo0 = spark.table('db_d170_bbe_in_iws.il_tmagic_fiberOnLocation_demo2_mt')
df_FoL_demo0.show()

