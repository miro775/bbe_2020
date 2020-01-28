from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from datetime import datetime

ts = datetime.now().strftime('%Y%m%d%H%M')

spark = SparkSession \
    .builder \
    .appName("miro_bbe_session_{ts}") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .getOrCreate()

tmp = spark.sql("select count(*) from db_d170_bbe_iws_pwr.IL_TMagic_jsoninput_ET")
tmp.show(1)
