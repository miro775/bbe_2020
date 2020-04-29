
import de.telekom.bdmp.pyfw.etl_framework.util as util
from de.telekom.bdmp.pyfw.etl_framework.iprocess import IProcess
from de.telekom.bdmp.pyfw.etl_framework.dfcreator import DfCreator
from de.telekom.bdmp.bbe.common.bdmp_constants import WF_AL2CL, DB_BBE_BASE, DB_BBE_CORE

from de.telekom.bdmp.bbe.common.tmagic_json_paths import *
import de.telekom.bdmp.bbe.common.functions as Func

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

#from pyspark.sql.functions import from_json
#from pyspark.sql.functions import from_unixtime
#from pyspark.sql.functions import to_timestamp
#from pyspark.sql.functions import col
#from pyspark.sql.functions import lit
from datetime import datetime

JOIN_LEFT_OUTER = 'left_outer'



class PsoToClProcess(IProcess):


    def __init__(self, save_dfs_if_exc=False, persist_result_dfs=False):
        """
        Constructor initialize the process

        """
        self._etl_process_name = 'proc_f_presalesorder'
        self._db_in = DB_BBE_BASE
        self._in_table_name = 'al_gigabit_message_mt'

        self._db_out = DB_BBE_CORE
        self._out_table_name = 'cl_f_order_mt'

        self._tmagic_messagetype = 'VVM - PreSalesOrder'
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
        Logic of the whole process,  PSO
        """


        df_input = in_dfs

        #self.log.debug('### logic of process \'{0}\' started,get_max_value_from_process_tracking_table...'.format(self.name))

        # retrieve information from the tracking table
        current_tracked_value, tracked_col = Func.get_max_value_from_process_tracking_table(
            self.spark_app.get_spark(), self._etl_process_name, self._in_table_name, col_name=True)


        # if the "process" doesn't have  record in "cl_m_process_tracking_mt" table - this is problem
        if current_tracked_value is None:
            self.log.debug('### process {0}  doesnt have  record in [cl_m_process_tracking_mt] table, '
                'not found entry for: {1} , {2}'.format(self.name,self._etl_process_name,self._in_table_name))
            raise


        # filter "Fac v2" only messages, only uprocessed records (alc_dop from : process-tracking-table)
        # for full-process AL2CL, disable filter:  & (df_input[tracked_col] > current_tracked_value)
        df_al = df_input.filter((df_input['messagetype'] == self._tmagic_messagetype) \
                                      & (df_input['Messageversion'] == '1') \
                                      #& (df_input[tracked_col] > current_tracked_value))

                                   & ((df_input['acl_id'] == '5530944') | (df_input['acl_id'] == '5530907'))  )  # 2rows for devlab debug

                                      #  testing only & ((df_input['acl_id'] == '5530944') | (df_input['acl_id'] == '5530907'))




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


        #  get schema from json-column 'jsonstruct'


        # printSchema only for DEBUG on devlab!
        #jsonschema_fac = self.spark_app.get_spark().read.json(df_al.rdd.map(lambda row: row.jsonstruct))
        #jsonschema_fac.printSchema()  # AttributeError: 'StructType' object has no attribute 'printSchema'

        # 'JSON.schema' as StructType ,  from column: jsonstruct
        jsonschema1 = self.spark_app.get_spark().read.json(df_al.rdd.map(lambda row: row.jsonstruct)).schema

        patern_timestamp_zulu = "yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'"
        time_zone_D="Europe/Berlin"

        # new dataframe , select columns for target table , using values from json....
        # if DataFrame is empty then error occured: pyspark.sql.utils.AnalysisException: 'No such struct field number in'
        df_al_json = df_al.withColumn('json_data', F.from_json(F.col('jsonstruct'), jsonschema1)) \
            .select(
            F.col('acl_id').alias('acl_id_int'),
            F.to_timestamp(F.col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
            F.col('messageversion'),



            F.col('bdmp_loadstamp'),
            F.col('bdmp_id'),
            F.col('bdmp_area_id')


        )

        #df_al_json.show(2, False)
        #df_al_json.printSchema()

        # this func. parse FaC serviceCharacteristic[] values
        # explode to : service_name, service_value

        #df_serv_ch = self.parse_jsn_serviceCharacteristic(df_al_json)



        # avoid exception,  added aliases for join-column: 'acl_id_int'
        # Constructing trivially true equals predicate, Perhaps you need to use aliases
        # spark.sql.AnalysisException: Reference 'acl_id_int' is ambiguous

        df_FAC2 = df_al_json.select(df_al_json['acl_id_int'],
                    df_al_json['acl_dop_ISO'],
                    df_al_json['messageversion'],
                    df_al_json['klsid_ps'],
                    df_al_json['eventid'],
                    df_al_json['requesttime_ISO'],
                    df_al_json['partyid'],
                    df_al_json['errormessage'],
                    df_al_json['eligibilityUnavailabilityReasonCode'],
                    df_al_json['eligibilityUnavailabilityReasonLabel'],
                    df_al_json['address_type'],



                    df_al_json['servicecharacteristic_array'],

                    df_al_json['bdmp_loadstamp'],
                    df_al_json['bdmp_id'],
                    df_al_json['bdmp_area_id']
                   )

        #df_FAC2.show(20,False)

        return  df_FAC2

    def handle_output_dfs(self, out_dfs):
        """
        Stores result data frames to output Hive tables
        """

        spark_io = util.ISparkIO.get_obj(self.spark_app.get_spark())

        # Read inputs
        df_cl_tmagic = out_dfs
        doing_Insert = False

        # test table  devlab: 'cl_tmagic_l0_vvmarea_mt'
        #spark_io.df2hive(df_cl_tmagic_vvm_area, DB_BBE_CORE, 'cl_f_vvmarea_mt', overwrite=True)
        # if dataframe doesn't have data - skip insert to table, no new data=no insert
        if df_cl_tmagic:
            spark_io.df2hive(df_cl_tmagic, DB_BBE_CORE, self._out_table_name , overwrite=False)
            doing_Insert = True

        Func.update_process_tracking_table(self.spark_app.get_spark(), self._etl_process_name, \
                                           self._in_table_name, self.max_acl_dop_val)

        Func.bbe_process_log_table(self.spark_app.get_spark(),WF_AL2CL, self._etl_process_name,'INFO',
                                   'end of process','insert table={0} ,doing_Insert={1}'. \
                                   format(self._out_table_name,doing_Insert),self._tmagic_messagetype)


        return df_cl_tmagic

    # this function parsing/explode  serviceCharacteristic ARRAY ,
    def parse_jsn_serviceCharacteristic(self, df_in_fac):

        # OK, no where filter
        df_serv_json = df_in_fac.select(df_in_fac['acl_id_int'],df_in_fac['json_serviceCharacteristic_array'])

        df_serv_json = df_serv_json.withColumn('explod_serviceCharacteristic',F.explode("json_serviceCharacteristic_array"))

        df_ServiceChar = df_serv_json.select(F.col('acl_id_int'),
                                    F.col('explod_serviceCharacteristic.name').alias('service_name'),
                                    F.col('explod_serviceCharacteristic.value').alias('service_value')
                                    )


        #df_ServiceChar.show(20,False)

        ####
        return df_ServiceChar