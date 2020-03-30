#  Oozie BDMP Python Action
#  https://mywiki.telekom.de/pages/viewpage.action?pageId=1001964637
#
#  oozie workflow:  bbe/workflow/util/wf_check_devlab.xml

from __future__ import print_function
# from de.telekom.bdmp.bdmf.base.main import BDMP
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


class Spark():
    # Reading the keytab details, KERBEROS
    keytab = "/nfs/bdmp/d171/env_setup/d171ins.keytab"
    principal = "d171ins@BDMP.DEVLAB.DE.TMO"

    # Creating spark session
    spark = SparkSession \
        .builder \
        .appName("Pyspark Hivemetastore") \
        .config("spark.history.kerberos.enabled", "true") \
        .config("spark.yarn.keytab", keytab) \
        .config("spark.yarn.principal", principal) \
        .config("spark.history.kerberos.keytab", keytab) \
        .config("spark.history.kerberos.principal", principal) \
        .enableHiveSupport() \
        .getOrCreate()

    # spark context:
    print("[ Hello world ] test the Spark context:")
    print(spark.version)

    # stoping the spark session
    spark.stop()


if __name__ == '__main__':
    pass