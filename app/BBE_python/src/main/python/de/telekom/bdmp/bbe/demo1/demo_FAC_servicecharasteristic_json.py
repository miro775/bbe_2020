from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
from pyspark.sql.types import *
from datetime import datetime
# import pyspark.sql.functions as F

ts = datetime.now().strftime('%Y%m%d%H%M')

# parsing ARRAY  , FAC v2 ,  TEST on devlab, miro
## issue with array....
## json from struct:  "availabilityCheckCalledEvent.eventPayload.serviceQualification.serviceQualificationItem[0].service.serviceCharacteristic"

## sample: basic json  struct is:  [{"name":"Ausbaustand Glasfaser","valueType":null,"value":"notPlanned","@baseType":null,"@schemaLocation":null,"@type":null}]

## json_serviceCharacteristic is:  array<struct<@baseType:string,@schemaLocation:string,@type:string,name:string,value:string,valueType:string>>

spark = SparkSession \
    .builder \
    .appName("miro_session_demo1") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .getOrCreate()


schema2_Serv = StructType([
                          StructField("name",            StringType()),
						  StructField("valueType",       StringType()),
                          StructField("value",           StringType()),
                          StructField("@baseType",       StringType()),
                          StructField("@schemaLocation", StringType()),
                          StructField("@type",           StringType())
])

# [{"name":"Ausbaustand Glasfaser","valueType":null,"value":"notPlanned","@baseType":null,"@schemaLocation":null,"@type":null}]


df_demo = spark.createDataFrame(  [(1, "AA", [1,2,3]), (2, "BB", [3,5])],     ["col1", "col2", "col3"])
df_demo.show()
df_demo.printSchema()
for name, dtype in df_demo.dtypes:
    print(name, dtype)


df = spark.createDataFrame([('Ausbaustand Glasfaser',None,'notPlanned',None,None,None)] , schema2_Serv)

df.show(1,False)
df.printSchema()
