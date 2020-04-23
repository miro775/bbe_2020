from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.types import *
from datetime import datetime
# import pyspark.sql.functions as F

#ts = datetime.now().strftime('%Y%m%d%H%M')



spark = SparkSession \
    .builder \
    .appName("miro_bbe_session_{ts}") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .getOrCreate()
# read data from tabl


'''

CREATE TABLE  db_d172_bbe_core_iws.cl_m_process_log_mt(
  workflow string, 
  process string,
  log_type string,
  log_time timestamp, 
  log_entry string,
  log_entry2 string,
  log_entry3 string)
STORED AS PARQUET;
'''

# simple "table" - all Columns are  STRING !

df_log = spark.createDataFrame([("AL2CL", "proc_dummy1", "INFO", "current_time1", "entry1", "entry2", "None" )],
                               ["workflow", "process", "log_type", "log_time", "log_entry", "log_entry2", "log_entry3"])

df_log.show(1,False)

'''
+--------+-----------+--------+-------------+---------+----------+----------+
|workflow|process    |log_type|log_time     |log_entry|log_entry2|log_entry3|
+--------+-----------+--------+-------------+---------+----------+----------+
|AL2CL   |proc_dummy1|INFO    |current_time1|entry1   |entry2    |None      |
+--------+-----------+--------+-------------+---------+----------+----------+
'''

schema_LogTabl = StructType([
                          StructField("workflow",   StringType()),
						  StructField("process",    StringType()),
                          StructField("log_type",   StringType()),
                          StructField("log_time",   TimestampType()),
                          StructField("log_entry",  StringType()),
                          StructField("log_entry2", StringType()),
						  StructField("log_entry3", StringType())
])


current_time1 = datetime.now()

df_log2 = spark.createDataFrame([("AL2CL", "proc_dummy2", "INFO", current_time1, "entry1", "entry2", "None" )],
                               schema_LogTabl)

df_log2.show(1,False)

'''
+--------+-----------+--------+--------------------------+---------+----------+----------+
|workflow|process    |log_type|log_time                  |log_entry|log_entry2|log_entry3|
+--------+-----------+--------+--------------------------+---------+----------+----------+
|AL2CL   |proc_dummy2|INFO    |2020-04-23 12:08:18.858698|entry1   |entry2    |None      |
+--------+-----------+--------+--------------------------+---------+----------+----------+
'''

# df_log2.write.insertInto("db_d172_bbe_core_iws.cl_m_process_log_mt",overwrite=False)



dct_logdata = dict()

dct_logdata['workflow'] = 'AL2CL'
dct_logdata['process'] = 'process2'
dct_logdata['log_type'] = 'DEBUG'
dct_logdata['log_time'] = datetime.now()
dct_logdata['log_entry'] = 'dct_logdata test'
dct_logdata['log_entry2'] = 'text2'
dct_logdata['log_entry3'] = 'text3'


print(dct_logdata)


sc = spark.sparkContext
rdd_1 = sc.parallelize([dct_logdata])
print(type(rdd_1))  #  <class 'pyspark.rdd.RDD'>

df_log3 = spark.createDataFrame(rdd_1, schema_LogTabl)

df_log3.show(1,False)

'''
+--------+--------+--------+--------------------------+----------------+----------+----------+
|workflow|process |log_type|log_time                  |log_entry       |log_entry2|log_entry3|
+--------+--------+--------+--------------------------+----------------+----------+----------+
|AL2CL   |process2|DEBUG   |2020-04-23 14:19:18.049906|dct_logdata test|text2     |text3     |
+--------+--------+--------+--------------------------+----------------+----------+----------+
'''