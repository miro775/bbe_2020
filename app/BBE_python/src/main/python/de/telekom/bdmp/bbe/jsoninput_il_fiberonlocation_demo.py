from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
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

df1 = spark.sql("select * from db_d170_bbe_iws_pwr.IL_TMagic_jsoninput_ET")


# filter , where 1 record  FibreOnLocation'
#df3 = df1.filter((df1['messagetype'] == 'DigiOSS - FibreOnLocation') & (df1['acl_id'] == '100053607'))

df3 = df1.filter(df1['messagetype'] == 'DigiOSS - FibreOnLocation')
#df3.show()

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
	col('messagetype'),
	col('messageversion'),
    col('json_data.creationDate'),
    col('json_data.modificationDate'),
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
    col('json_data.installationDate'),
    col('json_data.plannedInstallationBegin'),
    col('json_data.plannedInstallationEnd'),
    col('json_data.we'),
    col('json_data.ge'),
    col('json_data.sl'))

    # do not use show: .show()

#insert dataframe into table
df4jsn.write.insertInto('db_d170_bbe_in_iws.il_tmagic_fiberOnLocation_demo1_mt', overwrite=True)
#df4jsn.show()


# test, read and show data from table
df_FoL_demo0 = spark.table('db_d170_bbe_in_iws.il_tmagic_fiberOnLocation_demo1_mt')
df_FoL_demo0.show()

