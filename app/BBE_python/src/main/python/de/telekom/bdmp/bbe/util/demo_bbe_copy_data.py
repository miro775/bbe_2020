from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.types import *
from datetime import datetime
import pyspark.sql.functions as F



# Reading the keytab details, KERBEROS
keytab = "/nfs/bdmp/d170/env_setup/d170ins.keytab"
principal = "d170ins@BDMP.DEVLAB.DE.TMO"

# .config("spark.history.kerberos.enabled", "true") \
# .config("spark.yarn.keytab", keytab) \
# .config("spark.yarn.principal", principal) \
# .config("spark.history.kerberos.keytab", keytab) \
# .config("spark.history.kerberos.principal", principal) \

# Creating spark session
spark = SparkSession \
    .builder \
    .appName("BBE-d170-env-test-copy-data-miro") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .enableHiveSupport() \
    .getOrCreate()

# SOURCE, TARGET:
# select * from db_d170_bbe_in_iws.il_tmagic_json_test_vvmArea_ET;
# select * from db_d170_bbe_core_iws.cl_tmagic_vvm_area_mt;

# read data from table, BIG JSON 4records:  db_d170_bbe_in_iws.il_tmagic_json_test_vvmArea_ET
df1 = spark.sql("select * from db_d170_bbe_in_iws.il_tmagic_json_test_vvmArea_ET" )

# filter "vvm" only messages
df3 = df1.filter((df1['messagetype'] == 'DigiOSS - vvmArea')  & (df1['Messageversion'] == '1'))

_new_records_count = df3.count()
print('### source table, records count= \'{0}\' '.format(_new_records_count))

#  get schema from json-column 'jsonstruct'
jsonschema_vvm = spark.read.json(df3.rdd.map(lambda row: row.jsonstruct)).schema

# new dataframe , select columns for target table , using values from json....
df4jsn = df3.withColumn('json_data', F.from_json(F.col('jsonstruct'), jsonschema_vvm))\
    .select(
    F.col('acl_id'),
    F.to_timestamp(F.col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
    F.col('json_data.number').alias('vvmareaNumber') ,
    F.col('json_data.name').alias('vvmareaName') ,
    F.from_unixtime(F.col('json_data.creationDate')[0:10]).alias('creationDate_ISO') ,
    F.from_unixtime(F.col('json_data.modificationDate')[0:10]).alias('modificationDate_ISO'),
    F.col('json_data.areaType'),
    F.from_unixtime(F.col('json_data.rolloutDate')[0:10]).alias('rolloutDate_ISO'),
    F.col('json_data.areaStatus'),
    F.col('json_data.plannedArea'),
    F.from_unixtime(F.col('json_data.plannedFrom')[0:10]).alias('plannedFrom_ISO'),
    F.from_unixtime(F.col('json_data.plannedTo')[0:10]).alias('plannedTo_ISO'),
    )


#insert dataframe into table   db_d170_bbe_core_iws.cl_tmagic_vvm_area_mt
df4jsn.write.insertInto('db_d170_bbe_core_iws.cl_tmagic_vvm_area_mt', overwrite=True)

# test , just for demonstration, read and show data from target table
#df_vvm_demo0 = spark.table('db_d170_bbe_core_iws.cl_tmagic_vvm_area_mt')
#df_vvm_demo0.show()
