from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from datetime import datetime

ts = datetime.now().strftime('%Y%m%d%H%M')

tmagic_messagetype = 'VVM - PreSalesOrder'




spark0 = SparkSession \
    .builder \
    .appName("miro_bbe_session_pso") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .getOrCreate()

# read data from table
df_pso = spark0.sql("select * from  db_d172_bbe_base_iws.al_gigabit_message_mt")

# filter "pso" only messages
df_pso = df_pso.filter((df_pso['messagetype'] == tmagic_messagetype)  & (df_pso['Messageversion'] == '1'))
pso_records_count = df_pso.count()


ts_now = datetime.now().strftime('%Y%m%d%H%M')
print('{0} presaleorder messages, count={1}'.format(ts_now,pso_records_count))
#  202005012054 presaleorder messages, count=1064

# analyse JSON schema "read.json()" (struct) from all specific messages ,
json_schema_full = spark0.read.json(df_pso.rdd.map(lambda row: row.jsonstruct))
json_schema_full.printSchema()  # debug only
