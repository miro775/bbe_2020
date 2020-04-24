import de.telekom.bdmp.pyfw.etl_framework.util as util
from de.telekom.bdmp.pyfw.etl_framework.iprocess import IProcess
from de.telekom.bdmp.pyfw.etl_framework.dfcreator import DfCreator
from de.telekom.bdmp.bbe.common.bdmp_constants import WF_AL2CL , DB_BBE_BASE, DB_BBE_CORE, JOIN_LEFT_OUTER

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

        # filter "SO_E" only messages, only uprocessed records (alc_dop from : process-tracking-table)
        # for full-process AL2CL, disable filter:  & (df_input[tracked_col] > current_tracked_value)
        df_al = df_input.filter( ((df_input['messagetype'] == 'DigiOSS - ServiceOrderEvent') \
                                | (df_input['messagetype'] == 'SOSI - ServiceOrderEvents')) \
                                & (df_input['Messageversion'] == '1') \
                                & (df_input[tracked_col] > current_tracked_value))

                              #  & ((df_input['acl_id'] == '210149') | (
                              #    df_input['acl_id'] == '210167')))  # 2rows for devlab debug
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
        time_zone_D = "Europe/Berlin"

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

            F.expr(SOE_orderItem_array).alias('json_SOE_orderItem'),

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

        # explode
        df_service = self.parse_jsn_orderitem_array(df_al_json)

        df_SOE = df_al_json \
            .join(df_service, df_al_json['acl_id_int'] == df_service['acl_id_int'], JOIN_LEFT_OUTER) \
            .select(
             df_al_json['acl_id_int'],
             df_al_json['acl_dop_ISO'],

             df_al_json['SO_ID'],
             df_al_json['externalid_ps'],
             df_al_json['priority'],
             df_al_json['description'],
             df_al_json['category'],
             df_al_json['state'],

             # FIX, like:  to_utc_timestamp(df.eventTime_ts,  "Europe/Berlin")
             df_al_json['orderDate_iso'],
             df_al_json['completionDate_iso'],
             df_al_json['requestedStartDate_iso'],

             df_al_json['requestedCompletionDate_iso'],
             df_al_json['expectedCompletionDate_iso'],
             df_al_json['startDate_iso'],


             df_service['oi_id'],
             df_service['oi_state'],
             df_service['oi_action'],
             df_service['s_id'],
             df_service['s_name'],
             df_service['s_state'],

             df_al_json['newpseudo'],
             df_al_json['oldpseudo'],
             df_al_json['so_auftragsid'],

             df_al_json['bdmp_loadstamp'],
             df_al_json['bdmp_id'],
             df_al_json['bdmp_area_id']
        )

        return df_SOE

    def handle_output_dfs(self, out_dfs):


        #return None  #  temporary Quit

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



    # this function parsing/explode  serviceCharacteristic ARRAY , json_SOE_orderItem
    def parse_jsn_orderitem_array(self, df_in):

        # OK, no where filter
        df_json = df_in.select(df_in['acl_id_int'],df_in['json_SOE_orderItem'])


        df_json = df_json.withColumn('explod_SOE_orderItem',F.explode("json_SOE_orderItem"))

        df_Service = df_json.select(F.col('acl_id_int'),
                                    F.col('explod_SOE_orderItem.id').alias('oi_id'),
                                    F.col('explod_SOE_orderItem.state').alias('oi_state'),
        F.col('explod_SOE_orderItem.action').alias('oi_action'),
        F.col('explod_SOE_orderItem.service.id').alias('s_id'),
        F.col('explod_SOE_orderItem.service.name').alias('s_name'),
        F.col('explod_SOE_orderItem.service.serviceState').alias('s_state')
        )

        '''
          where OI_action = 'add'
          and S_name = 'GigaAnschluss'
          and S_State = 'active'
        '''

        df_Service = df_Service.filter(
            (df_Service['s_name'] == 'GigaAnschluss') & (df_Service['s_state'] == 'active') & (df_Service['oi_action'] == 'add'))


        df_Service.show(20,False)

        ####
        return df_Service