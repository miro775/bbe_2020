
import de.telekom.bdmp.pyfw.etl_framework.util as util
from de.telekom.bdmp.pyfw.etl_framework.iprocess import IProcess
from de.telekom.bdmp.pyfw.etl_framework.dfcreator import DfCreator
from de.telekom.bdmp.bbe.common.bdmp_constants import DB_BBE_BASE, DB_BBE_CORE
import de.telekom.bdmp.bbe.common.functions as Func

from pyspark.sql.types import *
import pyspark.sql.functions as F
#from pyspark.sql.functions import from_json
#from pyspark.sql.functions import from_unixtime
#from pyspark.sql.functions import to_timestamp
#from pyspark.sql.functions import col
#from pyspark.sql.functions import lit
from datetime import datetime



class TMagicToClProcess(IProcess):
    """
    Bestand process
    """

    def __init__(self, save_dfs_if_exc=False, persist_result_dfs=False):
        """
        Constructor initialize the process
        """
        self._etl_process_name = 'proc_f_vvmarea'
        self._db_in = DB_BBE_BASE
        self._in_table_name = 'al_gigabit_message_mt'

        self._db_out = DB_BBE_CORE
        self._out_table_name = 'cl_f_vvmarea_mt'


        IProcess.__init__(self, self._etl_process_name, self._in_table_name, self._out_table_name,
                             save_dfs_if_exc=save_dfs_if_exc, persist_result_dfs=persist_result_dfs)

        self._max_acl_dop_val = 0


    def prepare_input_dfs(self, in_dfs):
        """
        Preparation of input data frames
        """

        # Df creator class
        df_creator = DfCreator(self.spark_app.get_spark())

        # DWHM
        #df_input_vvmarea = df_creator.get_df(database=DB_BBE_BASE, table='al_gigabit_message_mt')
        df_input_vvmarea = df_creator.get_df(self._db_in,  self._in_table_name)


        return df_input_vvmarea

    def logic(self, in_dfs):
        """
        Logic of the whole process
        """

        df_input_vvmarea = in_dfs

        # retrieve information from the tracking table
        current_tracked_value, tracked_col = Func.get_max_value_from_process_tracking_table(
            self.spark_app.get_spark(), self._etl_process_name, self._in_table_name, col_name=True)

        # compute max value of acl_dop - needed for next transformation
        self._max_acl_dop_val = df_input_vvmarea.agg(F.max(df_input_vvmarea[tracked_col]).alias('max')).collect()[0][0]



        # filter "vvm" only messages, only uprocessed records (alc_dop from : process-tracking-table)
        df3 = df_input_vvmarea.filter((df_input_vvmarea['messagetype'] == 'DigiOSS - vvmArea') \
                                      & (df_input_vvmarea['Messageversion'] == '1') \
                                      & (df_input_vvmarea[tracked_col] > current_tracked_value))

        #  get schema from json-column 'jsonstruct'

        #jsonschema_vvm = spark.read.json(df3.rdd.map(lambda row: row.jsonstruct)).schema

        jsonschema_vvm = self.spark_app.get_spark().read.json(df3.rdd.map(lambda row: row.jsonstruct)).schema

        # new dataframe , select columns for target table , using values from json....
        df4jsn = df3.withColumn('json_data', F.from_json(F.col('jsonstruct'), jsonschema_vvm)) \
            .select(
            F.col('acl_id').alias('acl_id_int'),
            F.to_timestamp(F.col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
            F.col('messageversion'),
            F.col('json_data.number').alias('number'),
            F.col('json_data.name').alias('name'),
            F.lit(None).cast(BooleanType()).alias('is_reporting_relevant'),
            F.from_unixtime(F.col('json_data.creationDate')[0:10]).alias('creationDate_ISO'),
            F.from_unixtime(F.col('json_data.modificationDate')[0:10]).alias('modificationDate_ISO'),
            F.col('json_data.areaType'),
            F.from_unixtime(F.col('json_data.rolloutDate')[0:10]).alias('rolloutDate'),
            F.col('json_data.areaStatus'),
            F.col('json_data.plannedArea'),
            F.from_unixtime(F.col('json_data.plannedFrom')[0:10]).alias('plannedFrom_ISO'),
            F.from_unixtime(F.col('json_data.plannedTo')[0:10]).alias('plannedTo_ISO'),

            F.col('bdmp_loadstamp'),
            F.col('bdmp_id'),
            F.col('bdmp_area_id')


        )

        #common_transform = CommonTransform()

        #df_cl_d_dwhm_bestand_ps = common_transform.trans_al2cl(df_al_d_dwhm_bestand_ps)
        #df_cl_d_dwhm_push_ps = common_transform.trans_al2cl(df_al_d_dwhm_push_ps)

        #return [df_cl_d_dwhm_bestand_ps, df_cl_d_dwhm_push_ps]
        return  df4jsn

    def handle_output_dfs(self, out_dfs):
        """
        Stores result data frames to output Hive tables
        """

        spark_io = util.ISparkIO.get_obj(self.spark_app.get_spark())

        # Read inputs
        df_cl_tmagic_vvm_area = out_dfs

        # test table  devlab: 'cl_tmagic_l0_vvmarea_mt'
        #spark_io.df2hive(df_cl_tmagic_vvm_area, DB_BBE_CORE, 'cl_f_vvmarea_mt', overwrite=True)
        spark_io.df2hive(df_cl_tmagic_vvm_area, DB_BBE_CORE, self._out_table_name , overwrite=True)

        Func.update_process_tracking_table(self.spark_app.get_spark(), self._etl_process_name, \
                                           self._in_table_name, self._max_acl_dop_val)


        return df_cl_tmagic_vvm_area

