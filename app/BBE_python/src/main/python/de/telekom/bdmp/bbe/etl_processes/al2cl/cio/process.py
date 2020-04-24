
import de.telekom.bdmp.pyfw.etl_framework.util as util
from de.telekom.bdmp.pyfw.etl_framework.iprocess import IProcess
from de.telekom.bdmp.pyfw.etl_framework.dfcreator import DfCreator
from de.telekom.bdmp.bbe.common.bdmp_constants import WF_AL2CL, DB_BBE_BASE, DB_BBE_CORE
import de.telekom.bdmp.bbe.common.functions as Func

from pyspark.sql.types import *
import pyspark.sql.functions as F
#from pyspark.sql.functions import from_json
#from pyspark.sql.functions import from_unixtime
#from pyspark.sql.functions import to_timestamp
#from pyspark.sql.functions import col
#from pyspark.sql.functions import lit
from datetime import datetime


# CIOToClProcess
class CIOToClProcess(IProcess):
    """
    Bestand process
    """

    def __init__(self, save_dfs_if_exc=False, persist_result_dfs=False):
        """
        Constructor initialize the process
        """
        self._etl_process_name = 'proc_f_cio'
        self._db_in = DB_BBE_BASE
        self._in_table_name = 'al_gigabit_message_mt'

        self._db_out = DB_BBE_CORE
        self._out_table_name = 'cl_f_cio_mt'

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

        df_input = df_creator.get_df(self._db_in,  self._in_table_name)

        self.log.debug('### Preparation of input data frames of process \'{0}\' started'.format(self.name))


        return df_input

    def logic(self, in_dfs):
        """
        Logic of the whole process
        """

        df_input = in_dfs

        # retrieve information from the tracking table
        current_tracked_value, tracked_col = Func.get_max_value_from_process_tracking_table(
            self.spark_app.get_spark(), self._etl_process_name, self._in_table_name, col_name=True)

        # compute max value of acl_dop - needed for next transformation
        self.max_acl_dop_val = df_input.agg(F.max(df_input[tracked_col]).alias('max')).collect()[0][0]

        self.log.debug('### logic of process \'{0}\' started, current_tracked_value={1}, max_acl_dop={2}'.\
                       format(self.name,current_tracked_value,self.max_acl_dop_val))

        Func.bbe_process_log_table(self.spark_app.get_spark(),WF_AL2CL, self._etl_process_name,'INFO',
                                   'logic of process started','current_tracked_value={0}, max_acl_dop={1}'. \
                                   format(current_tracked_value, self.max_acl_dop_val),'100')

        # 'IBT - CustomerInstallationOrderEvent'
        # filter "CIO" only messages, only uprocessed records (alc_dop from : process-tracking-table)
        df_al = df_input.filter((df_input['messagetype'] == 'IBT - CustomerInstallationOrderEvent') \
                                       & ((df_input['Messageversion'] == '1') | (df_input['Messageversion'] == '2')) \
                                      # & (df_input[tracked_col] > current_tracked_value))
        & ((df_input['acl_id'] == '200876') | ( df_input['acl_id'] == '200877') | ( df_input['acl_id'] == '100053771')) )
        # 3rows for devlab debug  message v1='100053771'

        self.new_records_count = df_al.count()
        self.log.debug('### in_df, records count= \'{0}\'  ,process {1},'.format(self.new_records_count,self.name))

        Func.bbe_process_log_table(self.spark_app.get_spark(),WF_AL2CL, self._etl_process_name,'INFO',
                                   'logic of process','new_records_count={0}'. \
                                   format(self.new_records_count),'110')

        # IF DataFrame is empty , do not parse Json , no new data
        # "df_al.rdd.isEmpty()" ? - this can be performance problem ?!
        if self.new_records_count==0:
            self.log.debug('### logic of process \'{0}\' , input-dataFrame is empty, no new data'.format(self.name))
            return None


        #  get schema from json-column 'jsonstruct'

        #jsonschema_vvm = spark.read.json(df3.rdd.map(lambda row: row.jsonstruct)).schema

        patern_timestamp_zulu = "yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'"
        time_zone_D = "Europe/Berlin"

        jsonschema = self.spark_app.get_spark().read.json(df_al.rdd.map(lambda row: row.jsonstruct)).schema

        # new dataframe , select columns for target table , using values from json....
        # if DataFrame is empty then error occured: pyspark.sql.utils.AnalysisException: 'No such struct field number in'
        df_al_json = df_al.withColumn('json_data', F.from_json(F.col('jsonstruct'), jsonschema)) \
            .select(
            F.col('acl_id').alias('acl_id_int'),
            F.to_timestamp(F.col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
            F.col('messageversion'),

            F.col('json_data.eventType').alias('eventType'),
            F.to_utc_timestamp(F.to_timestamp(F.col('json_data.eventDateTime'), patern_timestamp_zulu), time_zone_D )
                .alias('eventDateTime'),
            F.to_utc_timestamp(F.to_timestamp(F.col('json_data.eventTime'), patern_timestamp_zulu), time_zone_D )
                .alias('eventTime'),
            #F.col('json_data.eventDateTime').alias('eventDateTime'),
            #F.col('json_data.eventTime').alias('eventTime'),

            F.col('json_data.eventPayload.cioBid').alias('ciobid_ps'),
            F.lit(None).alias('psoid_ps'),
            F.col('json_data.eventPayload.klsId').alias('klsId_ps'),
            F.col('json_data.eventPayload.projectBid').alias('projectBid'),
            F.col('json_data.eventPayload.homeId').alias('homeId_ps'),
            F.lit(None).alias('lasteventType'),
            F.lit(None).alias('creationdate'),
            F.lit(None).alias('cancelationdate'),
            F.lit(None).alias('completeddate'),
            F.col('json_data.eventPayload.cioStatus').alias('cioStatus'),
            F.col('json_data.eventPayload.carrierId').alias('carrierId'),
            F.col('json_data.eventPayload.cancellationReason').alias('cancellationReason'),
            F.col('json_data.eventPayload.sourceType').alias('sourceType'),
            F.col('json_data.partyId').alias('partyID'),
            F.col('json_data.eventPayload.externalOrderId').alias('externalOrderId_ps'),

            F.col('bdmp_loadstamp'),
            F.col('bdmp_id'),
            F.col('bdmp_area_id')
        )


        #df_al_json.show(3,False)

        return  df_al_json



    def handle_output_dfs(self, out_dfs):
        """
        Stores result data frames to output Hive tables
        """

        spark_io = util.ISparkIO.get_obj(self.spark_app.get_spark())

        # Read inputs
        df_out = out_dfs
        doing_Insert = False


        # if dataframe doesn't have data - skip insert to table, no new data=no insert
        if df_out :
            spark_io.df2hive(df_out, DB_BBE_CORE, self._out_table_name , overwrite=False)
            doing_Insert = True

        Func.update_process_tracking_table(self.spark_app.get_spark(), self._etl_process_name, \
                                           self._in_table_name, self.max_acl_dop_val)

        Func.bbe_process_log_table(self.spark_app.get_spark(),WF_AL2CL, self._etl_process_name,'INFO',
                                   'end of process','insert table={0} ,doing_Insert={1}'. \
                                   format(self._out_table_name,doing_Insert),'300')


        return df_out

