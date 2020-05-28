import pyspark.sql.functions as F  # lit, col, create_map

from pyspark.sql import SparkSession

from pyspark.sql import Row

import pyspark.sql.types as T
from itertools import chain

sparkX = SparkSession \
    .builder \
    .appName("miro_session_demo_map") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .getOrCreate()

df_demo1 = sparkX.createDataFrame([(1, "AA", [1, 2, 3]), (2, "BB", [3, 5])], ["col1", "col2", "col3"])
df_demo1.show()
df_demo1.printSchema()
for name, dtype in df_demo1.dtypes:
    print(name, dtype)

'''

+----+----+---------+
|col1|col2|     col3|
+----+----+---------+
|   1|  AA|[1, 2, 3]|
|   2|  BB|   [3, 5]|
+----+----+---------+

root
 |-- col1: long (nullable = true)
 |-- col2: string (nullable = true)
 |-- col3: array (nullable = true)
 |    |-- element: long (containsNull = true)

col1 bigint
col2 string
col3 array<bigint>

'''

# https://stackoverflow.com/questions/44436856/explode-array-data-into-rows-in-spark

df_demo1.withColumn("col3_explod", F.explode(df_demo1.col3)).show()

df2 = sparkX.createDataFrame(
    data=[
        ('k1', 'k14', [1, 14, 141]),
    ],
    schema=T.StructType([
        T.StructField('key_1', T.StringType()),
        T.StructField('key_2', T.StringType()),
        T.StructField('aux_data', T.ArrayType(T.IntegerType())),
    ])
)

df2.show()

'''
+-----+-----+------------+
|key_1|key_2|    aux_data|
+-----+-----+------------+
|   k1|  k14|[1, 14, 141]|
+-----+-----+------------+
'''

structureData = [
    ("36636", "Finance", [3000, "USA"]),
    ("40288", "Finance", [5000, "IND"]),
    ("42114", "Sales", [3900, "USA"]),
    ("39192", "Marketing", [2500, "CAN"]),
    ("34534", "Sales", [6500, "USA"])
]

# [('36636', 'Finance', [3000, 'USA']), ('40288', 'Finance', [5000, 'IND']), ('42114', 'Sales', [3900, 'USA']), ('39192', 'Marketing', [2500, 'CAN']), ('34534', 'Sales', [6500, 'USA'])]


'''
# scala syntax? 
structureData = [
    Row("36636", "Finance", Row(3000, "USA")),
    Row("40288", "Finance", Row(5000, "IND")),
    Row("42114", "Sales", Row(3900, "USA")),
    Row("39192", "Marketing", Row(2500, "CAN")),
    Row("34534", "Sales", Row(6500, "USA"))
]
'''

print('----structureData----')
print(structureData)
print(type(structureData))

'''
# this is wrong, do not use the ArrayType in this case...
structureSchema = T.StructType([

    T.StructField("id", T.StringType()),
    T.StructField("dept", T.StringType()),
    T.StructField("properties", T.ArrayType(T.StructType([
        T.StructField("salary", T.IntegerType()),
        T.StructField("location", T.StringType())
    ])))
])
'''

# StructType(List(StructField(id,StringType,true),StructField(dept,StringType,true),StructField(properties,ArrayType(StructType(List(StructField(salary,IntegerType,true),StructField(location,StringType,true))),true),true)))
# TypeError: element in array field properties: StructType can not accept object '3000' in type <class 'str'>
# TypeError: element in array field properties: StructType can not accept object 3000 in type <class 'int'>


# this is OK
structureSchema = T.StructType([

    T.StructField("id", T.StringType()),
    T.StructField("dept", T.StringType()),
    T.StructField("properties", T.StructType([
        T.StructField("salary", T.IntegerType()),
        T.StructField("location", T.StringType())
    ]))
])

print('----structureSchema----')
print(structureSchema)
print(type(structureSchema))

# StructType(List(StructField(id,StringType,true),StructField(dept,StringType,true),StructField(properties,StructType(List(StructField(salary,IntegerType,true),StructField(location,StringType,true))),true)))
# <class 'pyspark.sql.types.StructType'>


df = sparkX.createDataFrame(structureData, structureSchema)

df.show()
df.printSchema()

for name, dtype in df.dtypes:
    print(name, dtype)

'''
#show:

+-----+---------+-----------+
|   id|     dept| properties|
+-----+---------+-----------+
|36636|  Finance|[3000, USA]|
|40288|  Finance|[5000, IND]|
|42114|    Sales|[3900, USA]|
|39192|Marketing|[2500, CAN]|
|34534|    Sales|[6500, USA]|
+-----+---------+-----------+

#printSchema:

root
 |-- id: string (nullable = true)
 |-- dept: string (nullable = true)
 |-- properties: struct (nullable = true)
 |    |-- salary: integer (nullable = true)
 |    |-- location: string (nullable = true)


#print (dtype):

id string
dept string
properties struct<salary:int,location:string>


'''

# pyspark.sql.functions.create_map(*cols)  # Creates a new map column.

# Parameters
# cols ,list of column names (string) or list of Column expressions that are grouped as key-value pairs,
# e.g. (key1, value1, key2, value2, ).


#  https://sparkbyexamples.com/spark/spark-sql-map-functions/
#  https://sparkbyexamples.com/spark/spark-dataframe-map-maptype-column/
#  https://stackoverflow.com/questions/41288622/pyspark-create-maptype-column-from-existing-columns

# index1 = df.schema.fieldIndex("properties")
# propSchema = df.schema(index1).dataType.asInstanceOf[T.StructType]

metric = F.create_map(list(chain(*(
    (F.lit(name), F.col(name)) for name in df.columns if "properties" in name
)))).alias("metric")

df2 = df.select('id', metric)
df2.show(20, False)

'''
+-----+---------------------------+
|id   |metric                     |
+-----+---------------------------+
|36636|[properties -> [3000, USA]]|
|40288|[properties -> [5000, IND]]|
|42114|[properties -> [3900, USA]]|
|39192|[properties -> [2500, CAN]]|
|34534|[properties -> [6500, USA]]|
+-----+---------------------------+
'''

#  AttributeError: 'list' object has no attribute 'items'
# mapping_expr = F.create_map([F.lit(x) for x in chain(*structureData.items())])
# df3 = df.withColumn("value", mapping_expr.getItem(F.col("properties")))


# https://stackoverflow.com/questions/36869134/pyspark-converting-a-column-of-type-map-to-multiple-columns-in-a-dataframe

# pyspark.sql.utils.AnalysisException: "cannot resolve 'explode(`properties`)' due to data type mismatch:
# input to function explode should be array or map type, not struct<salary:int,location:string>
#
# df4 = df.select("id", F.explode("properties"))
# df4.show(20,False)


df4 = df2.select("id", F.explode("metric"))
df4.show(20, False)
'''
+-----+----------+-----------+
|id   |key       |value      |
+-----+----------+-----------+
|36636|properties|[3000, USA]|
|40288|properties|[5000, IND]|
|42114|properties|[3900, USA]|
|39192|properties|[2500, CAN]|
|34534|properties|[6500, USA]|
+-----+----------+-----------+
'''

df5 = df.select("id", F.create_map(F.lit('prop_salary'), F.col('properties.salary')).alias('map_salary'))
df5.show(20, False)
'''
+-----+---------------------+
|id   |map_salary           |
+-----+---------------------+
|36636|[prop_salary -> 3000]|
|40288|[prop_salary -> 5000]|
|42114|[prop_salary -> 3900]|
|39192|[prop_salary -> 2500]|
|34534|[prop_salary -> 6500]|
+-----+---------------------+
'''

# this is hardcoded / explicitly list of columns for map: key-value
df6 = df.select("id", "dept", F.create_map(F.lit('key_salary'), F.col('properties.salary'),
                                           F.lit('key_location'), F.col('properties.location')
                                           ).alias('map_properties'))

df6.show(20, False)

'''
+-----+---------+-----------------------------------------+
|id   |dept     |map_properties                           |
+-----+---------+-----------------------------------------+
|36636|Finance  |[key_salary -> 3000, key_location -> USA]|
|40288|Finance  |[key_salary -> 5000, key_location -> IND]|
|42114|Sales    |[key_salary -> 3900, key_location -> USA]|
|39192|Marketing|[key_salary -> 2500, key_location -> CAN]|
|34534|Sales    |[key_salary -> 6500, key_location -> USA]|
+-----+---------+-----------------------------------------+
'''

df6.select("id", "dept", F.explode("map_properties")).show(20, False)

'''

+-----+---------+------------+-----+
|id   |dept     |key         |value|
+-----+---------+------------+-----+
|36636|Finance  |key_salary  |3000 |
|36636|Finance  |key_location|USA  |
|40288|Finance  |key_salary  |5000 |
|40288|Finance  |key_location|IND  |
|42114|Sales    |key_salary  |3900 |
|42114|Sales    |key_location|USA  |
|39192|Marketing|key_salary  |2500 |
|39192|Marketing|key_location|CAN  |
|34534|Sales    |key_salary  |6500 |
|34534|Sales    |key_location|USA  |
+-----+---------+------------+-----+


'''

# ? index1 = df.schema.fieldIndex("properties")  # AttributeError: 'StructType' object has no attribute 'fieldIndex'
# ? propSchema = df.schema(index1).dataType.asInstanceOf[T.StructType]

print('----dataframe df, columns, dtypes:----')
print(df.columns)
print(df.dtypes)
'''
['id', 'dept', 'properties']
[('id', 'string'), ('dept', 'string'), ('properties', 'struct<salary:int,location:string>')]
'''

map1 = F.create_map(list(chain(*(
    (F.lit(name), F.col(name)) for name in df.columns if "properties" in name
)))).alias("map1")

print(map1)
# Column<b'map(properties, properties) AS `map1`'>
