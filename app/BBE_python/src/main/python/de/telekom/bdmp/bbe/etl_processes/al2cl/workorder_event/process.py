
import de.telekom.bdmp.pyfw.etl_framework.util as util
from de.telekom.bdmp.pyfw.etl_framework.iprocess import IProcess
from de.telekom.bdmp.pyfw.etl_framework.dfcreator import DfCreator
from de.telekom.bdmp.bbe.common.bdmp_constants import WF_AL2CL, DB_BBE_BASE, DB_BBE_CORE, patern_timestamp_zulu,\
    patern_timestamp19_zulu, time_zone_D
import de.telekom.bdmp.bbe.common.functions as Func

from pyspark.sql.types import *
import pyspark.sql.functions as F
#from pyspark.sql.functions import from_json
#from pyspark.sql.functions import from_unixtime
#from pyspark.sql.functions import to_timestamp
#from pyspark.sql.functions import col
#from pyspark.sql.functions import lit
from datetime import datetime


# WOEToClProcess -  workorder_event
class WOEToClProcess(IProcess):


    def __init__(self, save_dfs_if_exc=False, persist_result_dfs=False):
        """
        Constructor initialize the process
        """
        self._etl_process_name = 'proc_f_workorderevent'
        self._db_in = DB_BBE_BASE
        self._in_table_name = 'al_gigabit_message_mt'

        self._db_out = DB_BBE_CORE
        self._out_table_name = 'cl_f_workorderevent_mt'

        self._tmagic_messagetype = 'DigiOSS - WorkOrderEvent'
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

        # if the "process" doesn't have  record in "cl_m_process_tracking_mt" table - this is problem
        if current_tracked_value is None:
            self.log.debug('### process {0}  doesnt have  record in [cl_m_process_tracking_mt] table, '
                'not found entry for: {1} , {2}'.format(self.name,self._etl_process_name,self._in_table_name))
            raise

        # analyse JSON schema "read.json()" (struct) from all specific messages , filter  messagetype
        # json_schema_full=DataFrame, json_schema_full.schema' as StructType
        df_these_messagetype_all = df_input.filter((df_input['messagetype'] == self._tmagic_messagetype) \
                                & (df_input['Messageversion'] == '1') )
        json_schema_full = self.spark_app.get_spark().read.json(df_these_messagetype_all.rdd.map(lambda row: row.jsonstruct))


        # 'IBT - CustomerInstallationOrderEvent'
        # filter "woe" only messages, only uprocessed records (alc_dop from : process-tracking-table)
        df_al = df_input.filter((df_input['messagetype'] == self._tmagic_messagetype) \
                                       & (df_input['Messageversion'] == '1' ) \
                                       & (df_input[tracked_col] > current_tracked_value))

        #& ((df_input['acl_id'] == '200876') | ( df_input['acl_id'] == '200877') | ( df_input['acl_id'] == '100053771')) )
        # 3rows for devlab debug  message v1='100053771'

        self.new_records_count = df_al.count()

        # compute max value of acl_dop - needed for next transformation
        self.max_acl_dop_val = df_al.agg(F.max(df_al[tracked_col]).alias('max')).collect()[0][0]

        self.log.debug('### logic of process \'{0}\' started, current_tracked_value={1}, max_acl_dop={2}, new_records_count={3}'.\
                       format(self.name,current_tracked_value,self.max_acl_dop_val,self.new_records_count))

        Func.bbe_process_log_table(self.spark_app.get_spark(),WF_AL2CL, self._etl_process_name,'INFO',
                                   'logic of process started','current_tracked_value={0}, max_acl_dop={1}, new_records_count={2}'. \
                                   format(current_tracked_value, self.max_acl_dop_val,self.new_records_count),self._tmagic_messagetype)


        # IF DataFrame is empty , do not parse Json , no new data
        # "df_al.rdd.isEmpty()" ? - this can be performance problem ?!
        if self.new_records_count==0:
            self.log.debug('### logic of process \'{0}\' , input-dataFrame is empty, no new data'.format(self.name))
            return None



        # new dataframe , select columns for target table , using values from json....
        # if DataFrame is empty then error occured: pyspark.sql.utils.AnalysisException: 'No such struct field number in'
        df_al_json = df_al.withColumn('json_data', F.from_json(F.col('jsonstruct'), json_schema_full.schema)) \
            .select(
            F.col('acl_id').alias('acl_id_int'),
            F.to_timestamp(F.col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
            F.col('messageversion'),


            F.col('json_data.eventid').alias('eventid'),
            F.col('json_data.eventsource').alias('eventsource'),
            F.col('json_data.eventstream').alias('eventstream'),
            F.to_utc_timestamp(F.to_timestamp(F.col('json_data.eventDateTime'), patern_timestamp_zulu), time_zone_D)
                .alias('eventdatetime'),
            #F.col('json_data.eventDateTime').alias('eventdatetime'),
            F.col('json_data.eventtype').alias('eventtype'),
            F.lit(None).alias('eventmetadata'),
            F.col('json_data.eventversion').alias('eventversion'),

            F.col('json_data.eventPayload.externalReference').alias('externalreference'),
            F.col('json_data.eventPayload.workorderid').alias('workorderid'),
            F.col('json_data.eventPayload.workordertype').alias('workordertype'),
            F.col('json_data.eventPayload.taskresolutioncode').alias('taskresolutioncode'),
            F.col('json_data.eventPayload.taskresolutionstatus').alias('taskresolutionstatus'),
            F.col('json_data.eventPayload.taskresolutioncomment').alias('taskresolutioncomment'),
            F.col('json_data.eventPayload.klsid').alias('klsid'),
            F.col('json_data.eventPayload.cplid').alias('cplid'),
            F.col('json_data.eventPayload.ciobid').alias('ciobid'),
            F.to_utc_timestamp(F.to_timestamp(F.col('json_data.eventPayload.completiondate'), patern_timestamp_zulu), time_zone_D)
                .alias('completiondate'),

            F.col('json_data.eventPayload.explorationprotocolbid').alias('explorationprotocolbid'),
            F.col('json_data.eventPayload.buildupdecision').alias('buildupdecision'),
            F.to_utc_timestamp(F.to_timestamp(F.col('json_data.eventPayload.posignaturetimestamp'), patern_timestamp_zulu), time_zone_D)
                .alias('posignaturetimestamp'),
            F.col('json_data.eventPayload.numberaccommodationunits').alias('numberaccommodationunits'),
            F.col('json_data.eventPayload.numberbusinessunits').alias('numberbusinessunits'),
            F.col('json_data.eventPayload.homeid').alias('homeid'),

            F.col('bdmp_loadstamp'),
            F.col('bdmp_id'),
            F.col('bdmp_area_id')
        )


        df_al_json.show(20,False)

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
                                   format(self._out_table_name,doing_Insert),self._tmagic_messagetype)


        return df_out

