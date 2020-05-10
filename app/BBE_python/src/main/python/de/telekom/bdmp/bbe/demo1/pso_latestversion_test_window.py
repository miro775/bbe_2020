#from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.types import *
import pyspark.sql.functions as F
#from pyspark.sql.dataframe import DataFrame

from pyspark.sql.window import Window

from datetime import datetime


patern_timestamp_zulu = "yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'"
patern_timestamp19_zulu = "yyyy-MM-dd\'T\'HH:mm:ss"
time_zone_D = "Europe/Berlin"




input_gigabit_table = 'db_d172_bbe_base_iws.al_gigabit_message_mt'
_tmagic_messagetype = 'VVM - PreSalesOrder'
#--   db_d172_bbe_core_iws.cl_f_presalesorder_mt_orderItem_test;


spark0 = SparkSession \
    .builder \
    .appName("miro_bbe_session_pso") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .getOrCreate()

# read data from table

df_input = spark0.sql("select * from  {0}".format(input_gigabit_table))



# analyse JSON schema "read.json()" (struct) from all specific messages , filter  messagetype
# json_schema_full=DataFrame, json_schema_full.schema' as StructType
df_these_messagetype_all = df_input.filter((df_input['messagetype'] == _tmagic_messagetype) \
                                           & (df_input['Messageversion'] == '1'))
json_schema_full = spark0.read.json(df_these_messagetype_all.rdd.map(lambda row: row.jsonstruct))
# json_schema_full.printSchema()  # debug only


# filter "PSO" only messages,

df_al = df_input.filter((df_input['messagetype'] == _tmagic_messagetype) \
                        & (df_input['Messageversion'] == '1'))

# parse data from JSON
df_al_json = df_al.withColumn('json_data', F.from_json(F.col('jsonstruct'), json_schema_full.schema)) \
    .select(
    F.col('acl_id').alias('acl_id_int'),
    F.col('acl_dop').alias('acl_dop'),
    F.to_timestamp(F.col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
    F.col('acl_loadnumber').alias('acl_loadnumber_int'),
    F.col('messagetype').alias('messagetype'),
    F.col('messageversion').alias('messageversion'),

    F.col('json_data.id').alias('presalesorderid_ps'),
    F.col('json_data.lastModifiedAt').alias('modification_date'),

    # truncate first 19chars like:  '2019-06-24T09:46:54'
    F.to_utc_timestamp(F.to_timestamp(F.col('json_data.lastModifiedAt')[0:19], patern_timestamp19_zulu),
                       time_zone_D).alias('lastmodifiedat_iso'),
    F.lit(None).alias('jsonstruct'),

)


'''

rankedEvents AS
(
select *, 
       ROW_NUMBER() OVER  (PARTITION BY preSalesOrderId_ps ORDER BY modification_date DESC, acl_ID_int desc, acl_DOP desc) AS rnk
from extractedEvents
)
select * from rankedEvents
where rnk = 1



'''

#df_al_json.show(5,False)

# test data, devlab, 1x PSO_ID
df_al_json = df_al_json.filter(df_al_json['presalesorderid_ps'] == 'P-J$s5!ebL${dtu@hUS')
df_al_json.show(20,False)

# "rnk" == "1" ,  otherwise TypeError: condition should be string or Column

df_al_json = df_al_json.select(
    F.col('acl_id_int'),
    F.col('acl_dop'),
    F.col('acl_dop_ISO'),
    F.col('acl_loadnumber_int'),
    F.col('messagetype'),
    F.col('messageversion'),
    F.col('presalesorderid_ps'),
    F.col('modification_date'),
    F.col('lastmodifiedat_iso'),
    F.col('jsonstruct'),
    F.row_number().over(
    Window.partitionBy("presalesorderid_ps").orderBy(F.col("lastmodifiedat_iso").desc(),
                                                     F.col("acl_id_int").desc(),
                                                     F.col("acl_dop").desc())
    ).alias("rnk"),
    F.lit(None).alias('bdmp_loadstamp'),
    F.lit(None).alias('bdmp_id'),
    F.lit(None).alias('bdmp_area_id')

)

df_al_json = df_al_json.filter(df_al_json['rnk'] == '1')

df_al_json.show(20,False)


# target:   db_d172_bbe_core_iws.cl_f_presalesorder_latestversion_mt
df_al_json.write.insertInto('db_d172_bbe_core_iws.cl_f_presalesorder_latestversion_mt', overwrite=True)