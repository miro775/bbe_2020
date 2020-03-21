#!/usr/bin/python
# coding: utf-8
"""
---------------------------------------------------------------------------
BBE
---------------------------------------------------------------------------

Common functions

"""

import pyspark.sql.functions as F
from de.telekom.bdmp.bdmf.base.environment import Environment
from de.telekom.bdmp.bbe.common.bdmp_constants import *
#import de.telekom.bdmp.pyfw.etl_framework.util as util  = logic cloned from ENT
import de.telekom.bdmp.pyfw.etl_framework.util as util


def update_process_tracking_table(spark, etl_process_name, table_name, max_tracking_value, col_name='acl_dop'):
    """
    Updates max_tracking_value in cl_m_process_tracking_mt table based on provided parameters
    it will overwrite partition of particular etl_process_name with new values

    :param spark: spark context
    :param etl_process_name: name of the spark process
    :param table_name: name of the table which process transform
    :param max_tracking_value: max value of processes data of col_name
    :param col_name: name of the column which was used for max_tracking_value
    :return:
    """
    if max_tracking_value and max_tracking_value is not None:
        spark_io = util.ISparkIO.get_obj(spark)

        # table_name,max_tracking_value,col_name,bdmp_loadstamp,bdmp_id,bdmp_area_id,etl_process_name
        columns = ['table_name', 'max_tracking_value', 'col_name', 'bdmp_loadstamp','bdmp_id','bdmp_area_id', 'etl_process_name']
        vals = [(table_name, max_tracking_value, col_name, '','','', etl_process_name)]

        df_process_partition = spark.createDataFrame(vals, columns)
        spark_io.df2hive(df_process_partition, DB_BBE_CORE, TABLE_BBE_PROCESS_TRACKING, overwrite=True)
        # disabled,miro,  invalidate_metadata(DB_BBE_CORE, TABLE_BBE_PROCESS_TRACKING)


def get_max_value_from_process_tracking_table(spark, etl_process_name, table_name, col_name=False):
    """
    Get max_tracking_value from BBE process_tracking table for given ETL process
    Function will return also tracked column name from tracking table as input for get_iterations
    based on boolean col_name

    :param spark:
    :param etl_process_name:
    :param table_name:
    :param col_name: defines if tracked column name is requested
    :return:
    """
    spark_io = util.ISparkIO.get_obj(spark)
    df_process_track = spark_io.hive2df(DB_BBE_CORE, TABLE_BBE_PROCESS_TRACKING)
    last_max_tracked_value = None
    tracked_col_name = ''

    if etl_process_name is not None:
        aux_list = df_process_track \
            .filter((df_process_track['etl_process_name'] == etl_process_name)
                    & (df_process_track['table_name'] == table_name)) \
            .select('max_tracking_value', 'col_name').collect()
        if len(aux_list) > 0:
            value_list = sorted(aux_list, reverse=True)[0]
            last_max_tracked_value = value_list[0]
            if col_name:
                tracked_col_name = value_list[1]

    if col_name:
        return last_max_tracked_value, tracked_col_name
    else:
        return last_max_tracked_value



def invalidate_metadata(db_name, table_name):
    """
    Method for invalidating particular table metadata
    env variables BDMP_IMPALASERVER_HOST and BDMP_IMPALASERVER_PORT needs to be setup

    :param db_name:
    :param table_name:
    :return:
    """
    #if not check_env_local():
    try:
        from de.telekom.bdmp.bdmf.hive.impalaengine import ImpalaEngine
        iengine = ImpalaEngine("{}".format(Environment().get('BDMP_OS_USER')))
        # iengine.execute('INVALIDATE METADATA {}.{}'.format(db_name, table_name))
        iengine.execute('REFRESH {}.{}'.format(db_name, table_name))
    except:
        print('Invalidate metadata failed')
        # TO.DO: in 19.1 move except to iprocess
