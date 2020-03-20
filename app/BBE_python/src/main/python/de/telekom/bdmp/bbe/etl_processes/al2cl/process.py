
import de.telekom.bdmp.pyfw.etl_framework.util as util
from de.telekom.bdmp.pyfw.etl_framework.iprocess import IProcess
from de.telekom.bdmp.pyfw.etl_framework.dfcreator import DfCreator
from de.telekom.bdmp.bbe.common.bdmp_constants import DB_BBE_BASE, DB_BBE_CORE, DB_BBE_IN

from pyspark.sql.functions import from_json
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from datetime import datetime



class TMagicToClProcess(IProcess):
    """
    Bestand process
    """

    def __init__(self, save_dfs_if_exc=False, persist_result_dfs=False):
        """
        Constructor initialize the process
        """
        #temporary using IL layer,DB_BBE_IN   not AL:  DB_BBE_BASE


        IProcess.__init__(self, 'TMagic', DB_BBE_BASE + ' - al tables', DB_BBE_CORE + ' - cl tables',
                          save_dfs_if_exc=save_dfs_if_exc, persist_result_dfs=persist_result_dfs)

    def prepare_input_dfs(self, in_dfs):
        """
        Preparation of input data frames
        """

        # Df creator class
        df_creator = DfCreator(self.spark_app.get_spark())

        # DWHM
        df_input_vvmarea = df_creator.get_df(database=DB_BBE_BASE,  table='al_gigabit_message_mt')
        #df_al_d_dwhm_push_ps = df_creator.get_df(database=DB_BBE_BASE,  table='al_d_dwhm_push_ps_mt')

        return df_input_vvmarea

    def logic(self, in_dfs):
        """
        Logic of the whole process
        """

        df_input_vvmarea = in_dfs

        # filter "vvm" only messages
        df3 = df_input_vvmarea.filter((df_input_vvmarea['messagetype'] == 'DigiOSS - vvmArea') & (df_input_vvmarea['Messageversion'] == '1'))

        #  get schema from json-column 'jsonstruct'

        #jsonschema_vvm = spark.read.json(df3.rdd.map(lambda row: row.jsonstruct)).schema

        jsonschema_vvm = self.spark_app.get_spark().read.json(df3.rdd.map(lambda row: row.jsonstruct)).schema

        # new dataframe , select columns for target table , using values from json....
        df4jsn = df3.withColumn('json_data', from_json(col('jsonstruct'), jsonschema_vvm)) \
            .select(
            col('acl_id').alias('acl_id_int'),
            to_timestamp(col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
            col('json_data.number').alias('number'),
            col('json_data.name').alias('name'),
            lit(None).cast(BooleanType()).alias('is_reporting_relevant'),
            from_unixtime(col('json_data.creationDate')[0:10]).alias('creationDate_ISO'),
            from_unixtime(col('json_data.modificationDate')[0:10]).alias('modificationDate_ISO'),
            col('json_data.areaType'),
            from_unixtime(col('json_data.rolloutDate')[0:10]).alias('rolloutDate'),
            col('json_data.areaStatus'),
            col('json_data.plannedArea'),
            from_unixtime(col('json_data.plannedFrom')[0:10]).alias('plannedFrom_ISO'),
            from_unixtime(col('json_data.plannedTo')[0:10]).alias('plannedTo_ISO'),

            col('bdmp_loadstamp'),
            col('bdmp_id'),
            col('bdmp_area_id')


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
        spark_io.df2hive(df_cl_tmagic_vvm_area, DB_BBE_CORE, 'cl_f_vvmarea_mt', overwrite=True)
        #spark_io.df2hive(df_cl_d_dwhm_push_ps, DB_BBE_CORE, 'cl_d_dwhm_push_ps_mt', overwrite=True)

        return df_cl_tmagic_vvm_area

