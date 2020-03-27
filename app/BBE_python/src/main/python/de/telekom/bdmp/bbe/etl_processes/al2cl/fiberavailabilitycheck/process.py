
import de.telekom.bdmp.pyfw.etl_framework.util as util
from de.telekom.bdmp.pyfw.etl_framework.iprocess import IProcess
from de.telekom.bdmp.pyfw.etl_framework.dfcreator import DfCreator
from de.telekom.bdmp.bbe.common.bdmp_constants import DB_BBE_BASE, DB_BBE_CORE

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



class FACToClProcess(IProcess):
    """
    Bestand process
    """

    def __init__(self, save_dfs_if_exc=False, persist_result_dfs=False):
        """
        Constructor initialize the process
        db_d171_bbe_core_iws.cl_f_fiberavailabilitycheck_mt
        """
        self._etl_process_name = 'proc_f_fiberavailabilitycheck_v2'
        self._db_in = DB_BBE_BASE
        self._in_table_name = 'al_gigabit_message_fac_mt'

        self._db_out = DB_BBE_CORE
        self._out_table_name = 'cl_f_fiberavailabilitycheck_mt'


        IProcess.__init__(self, self._etl_process_name, self._in_table_name, self._out_table_name,
                             save_dfs_if_exc=save_dfs_if_exc, persist_result_dfs=persist_result_dfs)

        self._max_acl_dop_val = 0
        self._new_records_count = 0


    def prepare_input_dfs(self, in_dfs):
        """
        Preparation of input data frames
        """

        # Df creator class
        df_creator = DfCreator(self.spark_app.get_spark())

        # DWHM
        #df_input_vvmarea = df_creator.get_df(database=DB_BBE_BASE, table='al_gigabit_message_mt')
        df_input = df_creator.get_df(self._db_in,  self._in_table_name)

        self.log.debug('### Preparation of input data frames of process \'{0}\' started'.format(self.name))


        return df_input

    def logic(self, in_dfs):
        """
        Logic of the whole process
        """

        # serviceCharacteristic values:
        var_AusbaustandGF = None       # name:"Ausbaustand Glasfaser"
        var_PlanBeginnGfAusbau = None  # name:"Geplanter Beginn Glasfaser-Ausbau"
        var_PlanEndeGfAusbau = None    # name:"Geplantes Ende Glasfaser-Ausbau"
        var_Kooperationspartner = None # name:"Kooperationspartner"
        var_Technologie = None         # name: "Technologie"




        df_input = in_dfs

        self.log.debug('### logic of process \'{0}\' started,get_max_value_from_process_tracking_table...'.format(self.name))

        # retrieve information from the tracking table
        #current_tracked_value, tracked_col = Func.get_max_value_from_process_tracking_table(
        #    self.spark_app.get_spark(), self._etl_process_name, self._in_table_name, col_name=True)

        # compute max value of acl_dop - needed for next transformation
        #self._max_acl_dop_val = df_input.agg(F.max(df_input[tracked_col]).alias('max')).collect()[0][0]

        #self.log.debug('### logic of process \'{0}\' started, current_tracked_value={1}, max_acl_dop={2}'.\
        #               format(self.name,current_tracked_value,self._max_acl_dop_val))

        # filter "vvm" only messages, only uprocessed records (alc_dop from : process-tracking-table)
        df_al = df_input.filter((df_input['messagetype'] == 'DigiOSS - FiberAvailabilityEvent V2') \
                                      & (df_input['Messageversion'] == '2') \

                                      & ((df_input['acl_id'] == '200049771') | (df_input['acl_id'] == '5932772'))
                                )
                                      #& (df_input['acl_id'] == '200049771')    ) #\
                                      #& (df_input[tracked_col] > current_tracked_value))



        #  testing 1 record, acl_id = '200049771'

        self._new_records_count = df_al.count()
        self.log.debug('### in_df, records count= \'{0}\' '.format(self._new_records_count))

        # IF DataFrame is empty , do not parse Json , no new data
        # "df_al.rdd.isEmpty()" ? - this can be performance problem ?!
        if df_al.rdd.isEmpty():
            self.log.debug('### logic of process \'{0}\' , input-dataFrame is empty, no new data'.format(self.name))
            return None


        #  get schema from json-column 'jsonstruct'


        # printSchema only for DEBUG on devlab!
        #jsonschema_fac = self.spark_app.get_spark().read.json(df_al.rdd.map(lambda row: row.jsonstruct))
        #jsonschema_fac.printSchema()  # AttributeError: 'StructType' object has no attribute 'printSchema'

        # 'JSON.schema' as StructType ,  from column: jsonstruct
        jsonschema1 = self.spark_app.get_spark().read.json(df_al.rdd.map(lambda row: row.jsonstruct)).schema



        # new dataframe , select columns for target table , using values from json....
        # if DataFrame is empty then error occured: pyspark.sql.utils.AnalysisException: 'No such struct field number in'
        df_al_json = df_al.withColumn('json_data', F.from_json(F.col('jsonstruct'), jsonschema1)) \
            .select(
            F.col('acl_id').alias('acl_id_int'),
            F.to_timestamp(F.col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
            F.col('messageversion'),

            F.expr( FAC_v2_klsid_ps ).alias('klsid_ps'),
            F.col( FAC_v2_eventid ).alias('eventid'),

            # temporary,  only date, truncated HH:MM:ss  because extra char "T" , fix it
            F.to_timestamp(F.col( FAC_v2_eventTime )[0:10],'yyyy-MM-dd').alias('requesttime_ISO'),

            F.col( FAC_v2_partyid ).alias('partyid'),
            F.lit(None).cast(StringType()).alias('errormessage'),

            F.expr( FAC_v2_eligibilityUnavailabilityReasonCode ).alias('eligibilityUnavailabilityReasonCode'),
            F.expr( FAC_v2_eligibilityUnavailabilityReasonLabel ).alias('eligibilityUnavailabilityReasonLabel'),

            # additional PARSING needed:
            F.expr( FAC_v2__place ).alias('address_type'),

            # not working: F.get_json_object('jsonstruct',FAC_v2__place_type).alias('address_type'),

            F.expr(FAC_v2__serviceCharacteristic_name).alias('serviceCharacteristic_name'),
            F.expr(FAC_v2__serviceCharacteristic_value).alias('serviceCharacteristic_value'),


            # another PARSING needed
            #F.expr( FAC_v2__serviceCharacteristic ).alias('ausbaustandgf'),
            # F.lit(None).alias('ausbaustandgf

            #F.lit(None).alias('planbeginngfausbau'),
            #F.lit(None).alias('planendegfausbau'),

            #F.lit(None).cast(StringType()).alias('kooperationspartner'),
            #F.lit(None).cast(StringType()).alias('technologie'),

            F.col('bdmp_loadstamp'),
            F.col('bdmp_id'),
            F.col('bdmp_area_id')


        )

        df_al_json.show(10, False)


        df_AusbaustandGF = df_al_json.select(df_al_json['acl_id_int'],
                                             df_al_json['serviceCharacteristic_name'],
                                             df_al_json['serviceCharacteristic_value'])

        df_AusbaustandGF.show(10,False)

        var_AusbaustandGF = df_AusbaustandGF.filter(df_AusbaustandGF['serviceCharacteristic_name'] =='Ausbaustand Glasfaser') \
        .select(df_al_json['acl_id_int'], df_AusbaustandGF['serviceCharacteristic_value'])

        var_AusbaustandGF.show()


        #res = df.select(get_json_object(df['info'], "$.name").alias('name'))
        #res = df.filter(get_json_object(df['info'], "$.name") == 'pat')

        #var_AusbaustandGF = df_AusbaustandGF.select(F.get_json_object(df_AusbaustandGF['serviceCharacteristic_json']), "$.name")
        #var_AusbaustandGF.show()




        #df2_json = df_al.withColumn('json_data', F.get_json_object(F.col('jsonstruct'), FAC_v2__serviceCharacteristic))
        #df2_json.show(1,False)

        # Debug, show 1.st record ,data, False=no truncate
        #df_al_json.show(1,False)

        #if(df_al_json._serviceCharacteristic_name=='Ausbaustand Glasfaser'):
        #    val_AusbaustandGF = df_al_json._serviceCharacteristic_value


        return  df_al_json

    def handle_output_dfs(self, out_dfs):
        """
        Stores result data frames to output Hive tables
        """

        spark_io = util.ISparkIO.get_obj(self.spark_app.get_spark())

        # Read inputs
        df_cl_tmagic = out_dfs

        # test table  devlab: 'cl_tmagic_l0_vvmarea_mt'
        #spark_io.df2hive(df_cl_tmagic_vvm_area, DB_BBE_CORE, 'cl_f_vvmarea_mt', overwrite=True)
        # if dataframe doesn't have data - skip insert to table, no new data=no insert
        if df_cl_tmagic:
          spark_io.df2hive(df_cl_tmagic, DB_BBE_CORE, self._out_table_name , overwrite=False)

        #Func.update_process_tracking_table(self.spark_app.get_spark(), self._etl_process_name, \
         #                                  self._in_table_name, self._max_acl_dop_val)


        return df_cl_tmagic

