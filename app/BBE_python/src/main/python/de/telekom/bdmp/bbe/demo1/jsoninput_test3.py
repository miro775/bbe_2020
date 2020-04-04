from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
from pyspark.sql.types import *
from datetime import datetime

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
df4 = df3.withColumn('jsonstruct', from_json(col('jsonstruct'), jsonschema_FoL)).select('jsonstruct.*')
#df4.show()


# test, show table
#df_FoL_miro = spark.table('db_d170_bbe_in_iws.il_tmagic_fiberOnLocation_miro1_mt')
#df_FoL_miro.show()



df_map2 = df3.rdd.map(lambda p: (
    p['acl_id'], p['acl_dop'], p['acl_dop_yyyymm'],
    p['acl_loadnumber'], p['messagetype'], p['messageversion'], 1579174,
    1579175, 'klsId-555', 'RESIDENTIAL-areaType'
))


schema2_FoL = StructType([StructField('acl_id_int', StringType()),
                          StructField('acl_dop_iso', StringType()),
                          StructField('acl_dop_yyyymm', StringType()),
                          StructField('acl_loadnumber', StringType()),
                          StructField('messagetype', StringType()),
                          StructField('messageversion', StringType()),
                          StructField('creationDate', IntegerType()),
                          StructField('modificationDate', IntegerType()),
                          StructField('klsId', StringType()),
                          StructField('areaType', StringType())
                         ])

#list_FoL_1record = ['1001','20200131','202001','loadNumber111','msgType FoL','messgVer 1',
#                    '1579174772080','1579175201634','klsID-555','RESIDENTIAL-area']

df_FoL_table = spark.createDataFrame(df_map2,  schema2_FoL )
#insert dataframe to table
df_FoL_table.write.insertInto( 'db_d170_bbe_in_iws.il_tmagic_fiberOnLocation_miro0_mt', overwrite=False)
df_FoL_table.show()