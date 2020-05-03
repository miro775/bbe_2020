#from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.types import *
import pyspark.sql.functions as F
#from pyspark.sql.dataframe import DataFrame

from datetime import datetime

#ts = datetime.now().strftime('%Y%m%d%H%M')

tmagic_messagetype = 'VVM - PreSalesOrder'
input_gigabit_table = 'db_d172_bbe_base_iws.al_gigabit_message_mt'




spark0 = SparkSession \
    .builder \
    .appName("miro_bbe_session_pso") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .getOrCreate()

# read data from table
#df_pso = spark0.sql("select * from  db_d172_bbe_base_iws.al_gigabit_message_mt")
df_pso = spark0.sql("select * from  {0}".format(input_gigabit_table))

# filter "pso" only messages
df_pso = df_pso.filter((df_pso['messagetype'] == tmagic_messagetype)  & (df_pso['Messageversion'] == '1'))
pso_records_count = df_pso.count()

ts_now = datetime.now().strftime('%Y%m%d%H%M')
print('{0} presaleorder messages, count={1}'.format(ts_now,pso_records_count))
# 202005020701 presaleorder messages, count=1064


#extra filter, testing only older PSO json messages type , before  Jun2019
# table on devlab contains records for date 2019-05-13
df_pso = df_pso.filter((df_pso['acl_dop'] < '20190601000000') )
pso_records_count = df_pso.count()

print('presaleorder messages, extra filter, count={0}'.format(pso_records_count))
# presaleorder messages, extra filter, count=35

# show 5 lines from DataFrame, truncated=False
df_pso.show(5,False)

# analyse JSON schema "read.json()" (struct) from all specific messages ,
json_schema_full = spark0.read.json(df_pso.rdd.map(lambda row: row.jsonstruct))
json_schema_full.printSchema()  # debug only,  output was 765 lines...! truncated anyway


# [(x, y) for x, y in json_pso_schema1_subset.dtypes if x == 'installationLocation']

# the PSO messagetype  "v1"  have 2 differnet json-schemas , from Jun2019  ".installationLocation"  instead of ".location"
struct_has_newer_pso__json_schema = False
struct_has_older_pso__json_schema = False
newer_PSO_JSON_struct_attribute = 'installationLocation'
older_PSO_JSON_struct_attribute = 'installationAddress'

for name, dtype in json_schema_full.dtypes:
    if name == newer_PSO_JSON_struct_attribute:
        struct_has_newer_pso__json_schema = True
        print('found newer_PSO_JSON_struct_attribute: {0}'.format(newer_PSO_JSON_struct_attribute))
    if name == older_PSO_JSON_struct_attribute:
        struct_has_older_pso__json_schema = True
        print('found older_PSO_JSON_struct_attribute: {0}'.format(older_PSO_JSON_struct_attribute))


'''
CREATE TABLE db_d172_bbe_core_iws.cl_x_presalesorder_tmp
  (
    acl_id_int INT COMMENT ''
	, acl_dop STRING
    , acl_dop_iso TIMESTAMP COMMENT ''
	, acl_loadnumber_int INT
	, messagetype STRING
    , presalesorderid_ps STRING COMMENT ''
    , state STRING COMMENT ''
	, createdat_iso TIMESTAMP COMMENT ''
    , lastmodifiedat_iso TIMESTAMP COMMENT ''
	, klsid_ps_0 STRING
	, klsid_ps_1 STRING
)
STORED AS PARQUET
'''

patern_timestamp_zulu   = "yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'"
patern_timestamp19_zulu = "yyyy-MM-dd\'T\'HH:mm:ss"

time_zone_D = "Europe/Berlin"

#   2020-03-04T17:48:02.081058Z

# new dataframe , select columns for target table , using values from json....
# if DataFrame is empty then error occured: pyspark.sql.utils.AnalysisException: 'No such struct field number in'
# REPLACEMENT "limited" json_pso_schema1_subset.schema  WITH "full" schema struct: json_schema_full.schema
df_al_json = df_pso.withColumn('json_data', F.from_json(F.col('jsonstruct'), json_schema_full.schema)) \
    .select(
    F.col('acl_id').alias('acl_id_int'),
    F.col('acl_dop').alias('acl_dop'),
    F.to_timestamp(F.col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
    F.col('acl_loadnumber').alias('acl_loadnumber_int'),
    F.col('messagetype'),

    F.col('json_data.id').alias('presalesorderid_ps'),
    F.col('json_data.state').alias('state'),

    F.col('json_data.createdAt').alias('createdat_iso0'),
    F.col('json_data.createdAt')[0:19].alias('createdat_iso1'),
    #F.col('json_data.lastModifiedAt').alias('lastmodifiedat_iso0'),

    F.to_utc_timestamp(F.to_timestamp(F.col('json_data.createdAt')[0:19], patern_timestamp19_zulu), time_zone_D)
    .alias('createdat_iso'),
    F.to_utc_timestamp(F.to_timestamp(F.col('json_data.lastModifiedAt')[0:19], patern_timestamp19_zulu), time_zone_D)
    .alias('lastmodifiedat_iso'),

    # this will work ONLY if JSON schema STRUCT know-contains ALL attributes
    F.col('json_data.installationAddress.klsId').alias('klsid_ps_0')          # older PSO,  before Jun 2019

    #F.col('json_data.installationLocation.address.klsId').alias('klsid_ps_1')  # newer PSO,  after Jun 2019
)

df_al_json.show(10,False)

#df_al_json.printSchema()


#insert dataframe into table
#df_al_json.write.insertInto('db_d172_bbe_core_iws.cl_x_presalesorder_tmp', overwrite=True)
