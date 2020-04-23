import de.telekom.bdmp.pyfw.etl_framework.util as util
from de.telekom.bdmp.pyfw.etl_framework.iprocess import IProcess
from de.telekom.bdmp.pyfw.etl_framework.dfcreator import DfCreator
from de.telekom.bdmp.bbe.common.bdmp_constants import WF_AL2CL , DB_BBE_BASE, DB_BBE_CORE

from de.telekom.bdmp.bbe.common.tmagic_json_paths import *
import de.telekom.bdmp.bbe.common.functions as Func

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

# from pyspark.sql.functions import from_json
# from pyspark.sql.functions import from_unixtime
# from pyspark.sql.functions import to_timestamp
# from pyspark.sql.functions import col
# from pyspark.sql.functions import lit
from datetime import datetime

JOIN_LEFT_OUTER = 'left_outer'


class SOEToClProcess(IProcess):

    def __init__(self, save_dfs_if_exc=False, persist_result_dfs=False):
        """
        Constructor initialize the process
        """
        self._etl_process_name = 'proc_f_serviceorder_event'
        self._db_in = DB_BBE_BASE
        self._in_table_name = 'al_gigabit_message_mt'

        self._db_out = DB_BBE_CORE
        self._out_table_name = 'cl_f_soevent_mt'

        self.max_acl_dop_val = 0
        self.new_records_count = 0

        IProcess.__init__(self, self._etl_process_name, self._in_table_name, self._out_table_name,
                          save_dfs_if_exc=save_dfs_if_exc, persist_result_dfs=persist_result_dfs)

    def prepare_input_dfs(self, in_dfs):
        """
        Preparation of input data frames
        """

        # Df creator class
        df_creator = DfCreator(self.spark_app.get_spark())

        df_input = df_creator.get_df(self._db_in, self._in_table_name)

        self.log.debug('### Preparation of input data frames of process \'{0}\' started'.format(self.name))

        return df_input

    def logic(self, in_dfs):
        """
        Logic of the whole process,  FAC v2
        """

        df_input = in_dfs

        self.log.debug(
            '### logic of process \'{0}\' started,get_max_value_from_process_tracking_table...'.format(self.name))

        # retrieve information from the tracking table
        current_tracked_value, tracked_col = Func.get_max_value_from_process_tracking_table(
            self.spark_app.get_spark(), self._etl_process_name, self._in_table_name, col_name=True)

        # compute max value of acl_dop - needed for next transformation
        self.max_acl_dop_val = df_input.agg(F.max(df_input[tracked_col]).alias('max')).collect()[0][0]

        self.log.debug('### logic of process \'{0}\' started, current_tracked_value={1}, max_acl_dop={2}'. \
                       format(self.name, current_tracked_value, self.max_acl_dop_val))

        Func.bbe_process_log_table(self.spark_app.get_spark(),WF_AL2CL, self._etl_process_name,'INFO',
                                   'logic of process started','current_tracked_value={0}, max_acl_dop={1}'. \
                                   format(current_tracked_value, self.max_acl_dop_val),'100')

        # messagetype = 'DigiOSS - ServiceOrderEvent' OR messagetype = 'SOSI - ServiceOrderEvents'

        # filter "vvm" only messages, only uprocessed records (alc_dop from : process-tracking-table)
        # for full-process AL2CL, disable filter:  & (df_input[tracked_col] > current_tracked_value)
        df_al = df_input.filter((df_input['messagetype'] == 'DigiOSS - ServiceOrderEvent') \
                                & (df_input['Messageversion'] == '1') \
                              & (df_input[tracked_col] > current_tracked_value))

                             #   & ((df_input['acl_id'] == '210149') | (
                             #       df_input['acl_id'] == '210167')))  # 2rows for devlab debug
        #devlab filter:   acl_id in ('210149' ,  '210167', '210169')

        self.new_records_count = df_al.count()
        self.log.debug('### in_df, records count= \'{0}\'  ,process {1},'.format(self.new_records_count, self.name))

        Func.bbe_process_log_table(self.spark_app.get_spark(),WF_AL2CL, self._etl_process_name,'INFO',
                                   'logic of process','new_records_count={0}'. \
                                   format(self.new_records_count),'110')

        # IF DataFrame is empty , do not parse Json , no new data
        # "df_al.rdd.isEmpty()" ? - this can be performance problem ?!
        if self.new_records_count == 0:
            self.log.debug('### logic of process \'{0}\' , input-dataFrame is empty, no new data'.format(self.name))
            return None

        #  get schema from json-column 'jsonstruct'

        # printSchema only for DEBUG on devlab!
        # jsonschema_fac = self.spark_app.get_spark().read.json(df_al.rdd.map(lambda row: row.jsonstruct))
        # jsonschema_fac.printSchema()  # AttributeError: 'StructType' object has no attribute 'printSchema'

        # 'JSON.schema' as StructType ,  from column: jsonstruct
        jsonschema1 = self.spark_app.get_spark().read.json(df_al.rdd.map(lambda row: row.jsonstruct)).schema

        patern_timestamp_zulu = "yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'"

        # new dataframe , select columns for target table , using values from json....
        # if DataFrame is empty then error occured: pyspark.sql.utils.AnalysisException: 'No such struct field number in'
        df_al_json = df_al.withColumn('json_data', F.from_json(F.col('jsonstruct'), jsonschema1)) \
            .select(
            F.col('acl_id').alias('acl_id_int'),
            F.to_timestamp(F.col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
            # F.col('messageversion'),
            # F.lit(None).alias('acl_dop_yyyymm'),

            F.expr(SOE_SO_ID).alias('SO_ID'),
            F.expr(SOE_externalId_ps).alias('externalid_ps'),
            F.expr(SOE_priority).alias('priority'),
            F.expr(SOE_description).alias('description'),
            F.expr(SOE_category).alias('category'),
            F.expr(SOE_state).alias('state'),

            # F.to_timestamp(F.col(SOE_orderDate)[0:10], 'yyyy-MM-dd').alias('orderDate'),
            F.to_timestamp(F.col(SOE_orderDate), patern_timestamp_zulu).alias('orderDate_iso'),
            F.to_timestamp(F.col(SOE_completionDate), patern_timestamp_zulu).alias('completionDate_iso'),
            F.to_timestamp(F.col(SOE_requestedStartDate), patern_timestamp_zulu).alias(
                'requestedStartDate_iso'),
            F.to_timestamp(F.col(SOE_requestedCompletionDate), patern_timestamp_zulu).alias(
                'requestedCompletionDate_iso'),
            F.to_timestamp(F.col(SOE_expectedCompletionDate), patern_timestamp_zulu).alias(
                'expectedCompletionDate_iso'),
            F.to_timestamp(F.col(SOE_startDate), patern_timestamp_zulu).alias('startDate_iso'),

            F.lit(None).alias('oi_id'),
            F.lit(None).alias('oi_state'),
            F.lit(None).alias('oi_action'),
            F.lit(None).alias('s_id'),
            F.lit(None).alias('s_name'),
            F.lit(None).alias('s_state'),
            F.lit(None).alias('newpseudo'),
            F.lit(None).alias('oldpseudo'),
            F.lit(None).alias('so_auftragsid'),

            F.col('bdmp_loadstamp'),
            F.col('bdmp_id'),
            F.col('bdmp_area_id')

        )

        df_al_json.show(20, False)

        return df_al_json

    def handle_output_dfs(self, out_dfs):
        """
        Stores result data frames to output Hive tables
        """
        self.log.debug(
            '###  handle_output_dfs, \'{0}\' ,max_acl_dop={1}, table={2}'.format(self.name, self.max_acl_dop_val,
                                                                                 self._out_table_name))

        spark_io = util.ISparkIO.get_obj(self.spark_app.get_spark())

        # Read inputs
        df_cl_tmagic = out_dfs
        doing_Insert=False

        # if dataframe doesn't have data - skip insert to table, no new data=no insert
        if df_cl_tmagic:
            spark_io.df2hive(df_cl_tmagic, DB_BBE_CORE, self._out_table_name, overwrite=False)
            doing_Insert = True

        Func.update_process_tracking_table(self.spark_app.get_spark(), self._etl_process_name, \
                                           self._in_table_name, self.max_acl_dop_val)


        Func.bbe_process_log_table(self.spark_app.get_spark(),WF_AL2CL, self._etl_process_name,'INFO',
                                   'end of process','insert table={0} ,doing_Insert={1}'. \
                                   format(self._out_table_name,doing_Insert),'300')

        return df_cl_tmagic


