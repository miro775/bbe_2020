#from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.types import *
import pyspark.sql.functions as F
#from pyspark.sql.dataframe import DataFrame

from datetime import datetime

input_gigabit_table = 'db_d172_bbe_core_iws.cl_f_presalesorder_mt'
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

df_al = df_input.filter((df_input['acl_id_int'] == 200655) | (df_input['acl_id_int'] == 200742))

json_schema_reduced = spark0.read.json(
    df_al.rdd.map(lambda row: row.orderitems_struct))
json_schema_reduced.printSchema()  # debug only


#  select  acl_id_int,orderitems_struct,  bdmp_area_id from  db_d172_bbe_core_iws.cl_f_presalesorder_mt where acl_id_int in ( 200655, 200742) ;

df_al = df_al.select(df_al['acl_id_int'],df_al['orderitems_struct'])
df_al.show(2,True)

'''

+----------+--------------------+
|acl_id_int|   orderitems_struct|
+----------+--------------------+
|    200742|[{"id":"PoH{S","o...|
|    200655|[{"id":"Pg3äS","o...|
+----------+--------------------+

'''

#  pyspark.sql.utils.AnalysisException: "cannot resolve 'explode(cl_f_presalesorder_mt.`orderitems_struct`)'
#  due to data type mismatch: input to function explode should be array or map type, not string;

# which schema?   json_schema_reduced.schema /  json_schema_full.schema
df_al_json2 = df_al.withColumn('json_data', F.from_json(F.col('orderitems_struct'), json_schema_reduced.schema)) \
    .select("json_data.*")
df_al_json2.show(2, False)

'''
        # ciastocny uspech, ked je iba jeden [] v array,tak to funguje pre ten jeden,,,,: inak asi nejak pouzit explode() ?
        select("json_data.*")
        
+----------------------------------------------------------------------------------------------------------------------------------------+-----+--------+--------------------------------+-------------------+-----------------+-----------+-----------------+
|conditions                                                                                                                              |id   |itemType|name                            |orderId            |performProcessing|pricingDate|productMaterialId|
+----------------------------------------------------------------------------------------------------------------------------------------+-----+--------+--------------------------------+-------------------+-----------------+-----------+-----------------+
|[[ZK01, 1492, MagentaZuhause XXL mit, 730, true, 69.95, 1001328737], [ZK02, 1493, MagentaZuhause XXL mit, 730, true, 79.95, 1001363672]]|PoH{S|service |MagentaZuhause XXL mit MagentaTV|P&${WjZqiN`L6SjljmS|true             |2020-03-17 |89987868         |
|null                                                                                                                                    |null |null    |null                            |null               |null             |null       |null             |
+----------------------------------------------------------------------------------------------------------------------------------------+-----+--------+--------------------------------+-------------------+-----------------+-----------+-----------------+


'''

#df_al_json2.printSchema()







