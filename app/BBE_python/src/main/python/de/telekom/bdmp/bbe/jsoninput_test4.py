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

df4jsn = df3.withColumn('json_data', from_json(col('jsonstruct'), jsonschema_FoL))

# show data from json-struct
#df4jsn = df3.withColumn('jsonstruct2', from_json(col('jsonstruct'), jsonschema_FoL)).select('jsonstruct.*')
#df4jsn.show()


# test, show table
#df_FoL_miro = spark.table('db_d170_bbe_in_iws.il_tmagic_fiberOnLocation_miro1_mt')
#df_FoL_miro.show()


#PipelinedRDD
df_map1 = df4jsn.rdd.map(lambda p: (
    p['acl_id'], p['acl_dop'], p['acl_dop_yyyymm'],
    p['acl_loadnumber'], p['messagetype'], p['messageversion'],
    p['json_data.creationDate'],
    p['json_data.modificationDate'],
    p['json_data.klsId'],
    p['json_data.areaType']

))


schema1_FoL = StructType([StructField('acl_id_int', StringType()),
                          StructField('acl_dop_iso', StringType()),
                          StructField('acl_dop_yyyymm', StringType()),
                          StructField('acl_loadnumber', StringType()),
                          StructField('messagetype', StringType()),
                          StructField('messageversion', StringType()),
                          StructField('creationDate', LongType()),
                          StructField('modificationDate', LongType()),
                          StructField('klsId', StringType()),
                          StructField('areaType', StringType())
                         ])



df_FoL_table = spark.createDataFrame(df_map1,  schema1_FoL)


'''
df_map2 = df4jsn.rdd.map(lambda p: (
    p['creationDate'], p['modificationDate'], p['klsId'], p['areaType']))

schema2_FoL = StructType([StructField('creationDate', IntegerType()),
                          StructField('modificationDate', IntegerType()),
                          StructField('klsId', StringType()),
                          StructField('areaType', StringType())
                         ])
'''




#insert dataframe to table
df_FoL_table.write.insertInto( 'db_d170_bbe_in_iws.il_tmagic_fiberOnLocation_demo0_mt', overwrite=False)
df_FoL_table.show()