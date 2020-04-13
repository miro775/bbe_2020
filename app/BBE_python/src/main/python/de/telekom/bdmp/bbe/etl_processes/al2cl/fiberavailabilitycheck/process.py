
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

JOIN_LEFT_OUTER = 'left_outer'



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
        Logic of the whole process,  FAC v2
        """


        df_input = in_dfs

        self.log.debug('### logic of process \'{0}\' started,get_max_value_from_process_tracking_table...'.format(self.name))

        # retrieve information from the tracking table
        current_tracked_value, tracked_col = Func.get_max_value_from_process_tracking_table(
            self.spark_app.get_spark(), self._etl_process_name, self._in_table_name, col_name=True)

        # compute max value of acl_dop - needed for next transformation
        self.max_acl_dop_val = df_input.agg(F.max(df_input[tracked_col]).alias('max')).collect()[0][0]

        self.log.debug('### logic of process \'{0}\' started, current_tracked_value={1}, max_acl_dop={2}'.\
                       format(self.name,current_tracked_value,self.max_acl_dop_val))

        # filter "Fac v2" only messages, only uprocessed records (alc_dop from : process-tracking-table)
        # for full-process AL2CL, disable filter:  & (df_input[tracked_col] > current_tracked_value)
        df_al = df_input.filter((df_input['messagetype'] == 'DigiOSS - FiberAvailabilityEvent V2') \
                                      & (df_input['Messageversion'] == '2') \
                                      & (df_input[tracked_col] > current_tracked_value))

                                   #& ((df_input['acl_id'] == '200049771') | (df_input['acl_id'] == '5932772'))  )  # 2rows for devlab debug

                                      #  testing only & ((df_input['acl_id'] == '200049771') | (df_input['acl_id'] == '5932772'))



        self.new_records_count = df_al.count()
        self.log.debug('### in_df, records count= \'{0}\'  ,process {1},'.format(self.new_records_count,self.name))

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
            F.expr( FAC_v2__place_0 ).alias('address_type'),

            # not working: F.get_json_object('jsonstruct',FAC_v2__place_type).alias('address_type'),

            # reading only first record [0]  directly...
            #F.expr(FAC_v2__serviceCharacteristic0_name).alias('serviceCharacteristic0_name'),
            #F.expr(FAC_v2__serviceCharacteristic0_value).alias('serviceCharacteristic0_value'),

            # read all json-struct <array> from serviceCharacteristic[]
            # !!!
            # varianta 2: syntax for F.get_json_object()  ,  dtype will be:  string
            # F.get_json_object('jsonstruct', FAC_v2__json_serviceCharacteristic_x2).alias('json_serviceCharacteristic'),
            # varianta 1 : syntax for F.expr() ,  dtype will be :  array<struct.....
            F.expr( FAC_v2__json_serviceCharacteristic_x1).alias('json_serviceCharacteristic_array'),



            # these columns values will be added  via left joins,,,
            F.lit(None).alias('ausbaustandgf'),

            F.lit(None).alias('planbeginngfausbau'),
            F.lit(None).alias('planendegfausbau'),

            F.lit(None).cast(StringType()).alias('kooperationspartner'),
            F.lit(None).cast(StringType()).alias('technologie'),

            F.lit(None).alias('servicecharacteristic_array'),

            F.col('bdmp_loadstamp'),
            F.col('bdmp_id'),
            F.col('bdmp_area_id')


        )

        #df_al_json.show(2, False)
        #df_al_json.printSchema()

        # this func. parse FaC serviceCharacteristic[] values
        # explode to : service_name, service_value
        df_serv_ch = self.parse_jsn_serviceCharacteristic(df_al_json)


        # add aliases to columns to have unique columns name....
        df_AusbaustandGF = df_serv_ch.filter(df_serv_ch['service_name'] =='Ausbaustand Glasfaser') \
        .select(df_serv_ch['acl_id_int'].alias('acl_id_01'), df_serv_ch['service_value'].alias('ausbaustandgf'))

        df_Kooperationspartner = df_serv_ch.filter(df_serv_ch['service_name'] =='Kooperationspartner') \
        .select(df_serv_ch['acl_id_int'].alias('acl_id_02'), df_serv_ch['service_value'].alias('kooperationspartner'))

        df_Technologie= df_serv_ch.filter(df_serv_ch['service_name'] =='Technologie') \
        .select(df_serv_ch['acl_id_int'].alias('acl_id_03'), df_serv_ch['service_value'].alias('technologie'))

        # DatumVon,  F.to_timestamp( "column"[0:10],'yyyy-MM-dd')
        df_DatumVon = df_serv_ch.filter(df_serv_ch['service_name'] =='DatumVon') \
        .select(df_serv_ch['acl_id_int'].alias('acl_id_04'),
                F.to_timestamp(df_serv_ch['service_value'][0:10],'yyyy-MM-dd').alias('datumvon'))

        # DatumBis,  F.to_timestamp( "column"[0:10],'yyyy-MM-dd')
        df_DatumBis = df_serv_ch.filter(df_serv_ch['service_name'] =='DatumBis') \
        .select(df_serv_ch['acl_id_int'].alias('acl_id_05'),
                F.to_timestamp(df_serv_ch['service_value'][0:10],'yyyy-MM-dd').alias('datumbis'))

        df_AusbaustandGF.show(20,False)
        df_Kooperationspartner.show(20,False)
        df_Technologie.show(20,False)
        df_DatumVon.show()
        df_DatumBis.show()



        # avoid exception,  added aliases for join-column: 'acl_id_int'
        # Constructing trivially true equals predicate, Perhaps you need to use aliases
        # spark.sql.AnalysisException: Reference 'acl_id_int' is ambiguous

        df_FAC2 = df_al_json \
            .join(df_AusbaustandGF, df_al_json['acl_id_int'] == df_AusbaustandGF['acl_id_01'], JOIN_LEFT_OUTER) \
            .join(df_Kooperationspartner, df_al_json['acl_id_int'] == df_Kooperationspartner['acl_id_02'],
                  JOIN_LEFT_OUTER) \
            .join(df_Technologie, df_al_json['acl_id_int'] == df_Technologie['acl_id_03'], JOIN_LEFT_OUTER) \
            .join(df_DatumVon, df_al_json['acl_id_int'] == df_DatumVon['acl_id_04'], JOIN_LEFT_OUTER) \
            .join(df_DatumBis, df_al_json['acl_id_int'] == df_DatumBis['acl_id_05'], JOIN_LEFT_OUTER) \
            .select(df_al_json['acl_id_int'],
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

                    df_AusbaustandGF['ausbaustandgf'],

                    #df_al_json['planbeginngfausbau'],
                    #df_al_json['planendegfausbau'],
                    df_DatumVon['datumvon'].alias('planbeginngfausbau'),
                    df_DatumBis['datumbis'].alias('planendegfausbau'),

                    df_Kooperationspartner['kooperationspartner'],

                    df_Technologie['technologie'],

                    df_al_json['servicecharacteristic_array'],

                    df_al_json['bdmp_loadstamp'],
                    df_al_json['bdmp_id'],
                    df_al_json['bdmp_area_id']
                   )

        df_FAC2.show(20,False)

        return  df_FAC2

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

        Func.update_process_tracking_table(self.spark_app.get_spark(), self._etl_process_name, \
                                           self._in_table_name, self.max_acl_dop_val)


        return df_cl_tmagic

    # this function parsing/explode  serviceCharacteristic ARRAY ,
    def parse_jsn_serviceCharacteristic(self, df_in_fac):

        # OK, no where filter
        df_serv_json = df_in_fac.select(df_in_fac['acl_id_int'],df_in_fac['json_serviceCharacteristic_array'])

        # # devlab filter:        where 'acl_id_int' = '5932772'  or  200049771
        # # devlab testing only here:
        #df_serv_json = df_in_fac.filter((df_in_fac['acl_id_int'] == '200049771') | (df_in_fac['acl_id_int'] == '5932772')).\
        #    select('acl_id_int', 'json_serviceCharacteristic_array')

        df_serv_json = df_serv_json.withColumn('explod_serviceCharacteristic',F.explode("json_serviceCharacteristic_array"))

        df_ServiceChar = df_serv_json.select(F.col('acl_id_int'),
                                    F.col('explod_serviceCharacteristic.name').alias('service_name'),
                                    F.col('explod_serviceCharacteristic.value').alias('service_value')
                                    )


        df_ServiceChar.show(20,False)

        ####
        return df_ServiceChar