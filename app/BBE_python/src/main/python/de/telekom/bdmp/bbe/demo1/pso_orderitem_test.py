#from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.types import *
import pyspark.sql.functions as F
#from pyspark.sql.dataframe import DataFrame

from datetime import datetime

input_gigabit_table = 'db_d174_bbe_core_iws.cl_f_presalesorder_mt'
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

# this test MUST work FINE anytime, 2x acl_id, 200655 + 200742  -  To-DO!
df_al = df_input.filter((df_input['acl_id_int'] == 200655) | (df_input['acl_id_int'] == 200742))

# df_al = df_input.filter((df_input['acl_id_int'] == 200655) )

json_schema_reduced = spark0.read.json(
    df_input.rdd.map(lambda row: row.orderitems_json))  # read ALL records from source table? df_input instead of df_al

json_schema_reduced.printSchema()  # debug only

'''

root
 |-- conditions: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- conditionType: string (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- invoiceText: string (nullable = true)
 |    |    |-- orderItemId: long (nullable = true)
 |    |    |-- performProcessing: boolean (nullable = true)
 |    |    |-- price: string (nullable = true)
 |    |    |-- telekomConditionId: string (nullable = true)
 |-- id: string (nullable = true)
 |-- itemType: string (nullable = true)
 |-- name: string (nullable = true)
 |-- orderId: string (nullable = true)
 |-- performProcessing: boolean (nullable = true)
 |-- pricingDate: string (nullable = true)
 |-- productMaterialId: string (nullable = true)

'''



# select  acl_id_int,orderitems_array,orderitems_json
# from  db_d174_bbe_core_iws.cl_f_presalesorder_mt where acl_id_int in ( 200655, 200742) ;

df_al = df_al.select(df_al['acl_id_int'],df_al['orderitems_json'])
df_al.show(2,True)

'''

# acl_id=200742 has only 1 orderItem in array:   
   MagentaZuhause XXL mit MagentaTV	= service 
 
# acl_id=200655 has 4 orderItems in array:
    MagentaZuhause Giga = service
    Media Receiver 401 schwarz = receiver
    Speedport Smart 3 = router
    Glasfaser-Modem	= ont

+----------+--------------------+
|acl_id_int|    orderitems_array|
+----------+--------------------+
|    200655|[[[[ZK01, 633,, 3...|
|    200742|[[[[ZK01, 1492, M...|
+----------+--------------------+

! orderitems_json = [{json in array}]

+----------+--------------------+
|acl_id_int|     orderitems_json|
+----------+--------------------+
|    200655|[{"id":"Pg3äS","o...|
|    200742|[{"id":"PoH{S","o...|
+----------+--------------------+

'''

#  pyspark.sql.utils.AnalysisException: "cannot resolve 'explode(cl_f_presalesorder_mt.`orderitems_struct`)'
#  due to data type mismatch: input to function explode should be array or map type, not string;

df_al_json2 = df_al.withColumn('json_data', F.from_json(F.col('orderitems_json'), json_schema_reduced.schema)) \
    .select("json_data.*")
df_al_json2.show(2, False)

'''
# ciastocny uspech, ked je iba jeden [] v array,tak to funguje pre ten jeden,,,,: inak asi nejak pouzit explode() ?
 select("json_data.*")
        

+----------------------------------------------------------------------------------------------------------------------------------------+-----+--------+--------------------------------+-------------------+-----------------+-----------+-----------------+
|conditions                                                                                                                              |id   |itemType|name                            |orderId            |performProcessing|pricingDate|productMaterialId|
+----------------------------------------------------------------------------------------------------------------------------------------+-----+--------+--------------------------------+-------------------+-----------------+-----------+-----------------+
|null                                                                                                                                    |null |null    |null                            |null               |null             |null       |null             |
|[[ZK01, 1492, MagentaZuhause XXL mit, 730, true, 69.95, 1001328737], [ZK02, 1493, MagentaZuhause XXL mit, 730, true, 79.95, 1001363672]]|PoH{S|service |MagentaZuhause XXL mit MagentaTV|P&${WjZqiN`L6SjljmS|true             |2020-03-17 |89987868         |
+----------------------------------------------------------------------------------------------------------------------------------------+-----+--------+--------------------------------+-------------------+-----------------+-----------+-----------------+


'''


# test,  extract first [] ??  'json_data.id[0]' -<<  ERROR  No such struct field id[0]
df_al_json3 = df_al.withColumn('json_data', F.from_json(F.col('orderitems_json'), json_schema_reduced.schema)) \
    .select(
    F.col('acl_id_int'),
    F.col('json_data.id') ,
    F.col('json_data.name'),  F.col('json_data.itemType'),
    F.col('json_data.orderId'),  F.col('json_data.productMaterialId'))
df_al_json3.show(2, False)

'''

+----------+-----+--------------------------------+--------+-------------------+-----------------+
|acl_id_int|id   |name                            |itemType|orderId            |productMaterialId|
+----------+-----+--------------------------------+--------+-------------------+-----------------+
|200655    |null |null                            |null    |null               |null             |
|200742    |PoH{S|MagentaZuhause XXL mit MagentaTV|service |P&${WjZqiN`L6SjljmS|89987868         |
+----------+-----+--------------------------------+--------+-------------------+-----------------+

'''




df_al_json2.printSchema()

'''

root
 |-- conditions: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- conditionType: string (nullable = true)
 |    |    |-- id: long (nullable = true)
 |    |    |-- invoiceText: string (nullable = true)
 |    |    |-- orderItemId: long (nullable = true)
 |    |    |-- performProcessing: boolean (nullable = true)
 |    |    |-- price: string (nullable = true)
 |    |    |-- telekomConditionId: string (nullable = true)
 |-- id: string (nullable = true)
 |-- itemType: string (nullable = true)
 |-- name: string (nullable = true)
 |-- orderId: string (nullable = true)
 |-- performProcessing: boolean (nullable = true)
 |-- pricingDate: string (nullable = true)
 |-- productMaterialId: string (nullable = true)


'''


#  regexp_replace(get_json_object(org.JsonStruct, '$.items'), '^\\[|\\]$', '')
# -- remove '[' at the beginning and remove ']' at the end

df_al_json4 = df_al.withColumn('json_data',
                               F.from_json(
                                   F.regexp_replace(F.col('orderitems_json'), '^\\[|\\]$' , '')
                                   , json_schema_reduced.schema )
                               ) \
    .select("json_data.*")
df_al_json4.show(2, False)

'''
# OK, this work: regexp_replace(F.col('orderitems_json'), '^\\[|\\]$' 
# remove array[] , "wrapper-array" ,  ok some progress,,,,
+--------------------------------------------------------------------------------------------------------------------------------------------+-----+--------+----------+----------------+------------------+--------------------------------+----------------+------------------+-------------------+-----------------+-----------+-----------------+
|conditions                                                                                                                                  |id   |itemType|lineNumber|monthlyCondition|monthlyConditionId|name                            |oneTimeCondition|oneTimeConditionId|orderId            |performProcessing|pricingDate|productMaterialId|
+--------------------------------------------------------------------------------------------------------------------------------------------+-----+--------+----------+----------------+------------------+--------------------------------+----------------+------------------+-------------------+-----------------+-----------+-----------------+
|[[ZK01, 633,, 352, true, 0.00, 1001111111,,], [ZC02, 634,, 352, true, 119.95, 1001458452,,]]                                                |Pg3äS|service |null      |null            |null              |MagentaZuhause Giga             |null            |null              |P!rk~ß$ü+KÄGRgJy85S|true             |2019-08-29 |89930562         |
|[[ZK01, 1492, MagentaZuhause XXL mit, 730, true, 69.95, 1001328737,,], [ZK02, 1493, MagentaZuhause XXL mit, 730, true, 79.95, 1001363672,,]]|PoH{S|service |null      |null            |null              |MagentaZuhause XXL mit MagentaTV|null            |null              |P&${WjZqiN`L6SjljmS|true             |2020-03-17 |89987868         |
+--------------------------------------------------------------------------------------------------------------------------------------------+-----+--------+----------+----------------+------------------+--------------------------------+----------------+------------------+-------------------+-----------------+-----------+-----------------+

'''





df_al_json5 = df_al.withColumn('json_data',
                               F.from_json(
                                   F.regexp_replace(F.col('orderitems_json'), '^\\[|\\]$' , '')
                                   , json_schema_reduced.schema )
                               ) \
    .select(
    F.col('acl_id_int'),
    F.col('json_data.id'),
    F.col('json_data.name'),
    F.col('json_data.itemType'),
    F.col('json_data.orderId'),
    F.col('json_data.productMaterialId'))
df_al_json5.show(10, False)

'''
+----------+-----+--------------------------------+--------+-------------------+-----------------+
|acl_id_int|id   |name                            |itemType|orderId            |productMaterialId|
+----------+-----+--------------------------------+--------+-------------------+-----------------+
|200655    |Pg3äS|MagentaZuhause Giga             |service |P!rk~ß$ü+KÄGRgJy85S|89930562         |
|200742    |PoH{S|MagentaZuhause XXL mit MagentaTV|service |P&${WjZqiN`L6SjljmS|89987868         |
+----------+-----+--------------------------------+--------+-------------------+-----------------+
'''


#
# explode:  Returns a new row for each element in the given array or map.
#
# cannot resolve 'explode(regexp_replace(cl_f_presalesorder_mt.`orderitems_json`, '^\\[|\\]$', ''))'
# due to data type mismatch: input to function explode should be array or map type, not string

#
'''
sql.utils.AnalysisException: "cannot resolve 'explode(jsontostructs(regexp_replace(cl_f_presalesorder_mt.`orderitems_json`, '^\\\\[|\\\\]$', '')))' 
due to data type mismatch: input to function explode should be array or map type, not struct<conditions:array<struct<conditionType:string,id:bigint,invoiceText:string,orderItemId:bigint,performProcessing:boolean,
>> toto  nefunguje:


df_al_json6 = df_al.withColumn('json_explod_data',
                               F.explode(
                                   F.from_json(
                                        F.regexp_replace(F.col('orderitems_json'), '^\\[|\\]$' , '')
                                       , json_schema_reduced.schema
                                   )
                                   )
                               ) \
    .select(
    F.col('acl_id_int'),
    F.col('json_explod_data.id'),
    F.col('json_explod_data.name'),
    F.col('json_explod_data.itemType'),
    F.col('json_explod_data.orderId'),
    F.col('json_explod_data.productMaterialId'))
df_al_json6.show(10, False)

'''







