
import de.telekom.bdmp.pyfw.etl_framework.util as util
from de.telekom.bdmp.pyfw.etl_framework.iprocess import IProcess
from de.telekom.bdmp.pyfw.etl_framework.dfcreator import DfCreator
from de.telekom.bdmp.bbe.common.bdmp_constants import WF_AL2CL, DB_BBE_BASE, DB_BBE_CORE, patern_timestamp19_zulu, time_zone_D

from de.telekom.bdmp.bbe.common.tmagic_json_paths import *
import de.telekom.bdmp.bbe.common.functions as Func

from pyspark.sql.types import *
import pyspark.sql.functions as F
from typing import List
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
        self._out_table_name = 'cl_f_presalesorder_mt'
        self._out_table_name_pso_orderitem  = 'cl_f_presalesorder_orderitem_mt'
        self._out_table_name_pso_history = 'cl_f_presalesorder_history_mt'

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

        # analyse JSON schema "read.json()" (struct) from all specific messages , filter  messagetype
        # json_schema_full=DataFrame, json_schema_full.schema' as StructType
        df_these_messagetype_all = df_input.filter((df_input['messagetype'] == self._tmagic_messagetype) \
                                                 & (df_input['Messageversion'] == '1'))
        json_schema_full = self.spark_app.get_spark().read.json(df_these_messagetype_all.rdd.map(lambda row: row.jsonstruct))
        #json_schema_full.printSchema()  # debug only


        # filter "PSO" only messages, only uprocessed records (alc_dop from : process-tracking-table)
        # for full-process AL2CL, disable filter:  & (df_input[tracked_col] > current_tracked_value)
        df_al = df_input.filter((df_input['messagetype'] == self._tmagic_messagetype) \
                                      & (df_input['Messageversion'] == '1') \
                                      & (df_input[tracked_col] > current_tracked_value))

        #& ((df_input['acl_id'] == '5530944') | (df_input['acl_id'] == '5530907') | (df_input['acl_id'] == '200753')))  # 3rows for devlab debug
        #& ((df_input['acl_id'] == '5530944') | (df_input['acl_id'] == '5530907')))  # 2rows for devlab debug, doesn't contains: customerDetails.telekomCustomerId

        # acl_id 200753 contains :customerDetails.telekomCustomerId





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



        #patern_timestamp_zulu = "yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'"
        #patern_timestamp19_zulu = "yyyy-MM-dd\'T\'HH:mm:ss"
        #time_zone_D="Europe/Berlin"

        # new dataframe , select columns for target table , using values from json....
        # if DataFrame is empty then error occured: pyspark.sql.utils.AnalysisException: 'No such struct field number in'
        # REPLACEMENT "limited" json_pso_schema1_subset.schema  WITH "full" schema struct: json_schema_full.schema

        # the PSO v1  has 2 diferent JSON-struct !!! ,   before Jun2019 and after Jun2019
        # .location -> .installationLocation
        # .installationAddress -> .installationLocation.address
        #  etc


        df_al_json = df_al.withColumn('json_data', F.from_json(F.col('jsonstruct'), json_schema_full.schema)) \
            .select(
            F.col('acl_id').alias('acl_id_int'),
            F.to_timestamp(F.col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
            F.col('acl_loadnumber').alias('acl_loadnumber_int'),
            F.col('messageversion'),

            F.col('json_data.id').alias('presalesorderid_ps'),
            F.col('json_data.state').alias('state'),
            F.col('json_data.customerLandlordRole').alias('customerlandlordrole'),
            F.col('json_data.connectionOnly').alias('connectiononly'),

            #truncate first 19chars like:  '2019-06-24T09:46:54'
            F.to_utc_timestamp(F.to_timestamp(F.col('json_data.createdAt')[0:19], patern_timestamp19_zulu), time_zone_D)
                .alias('createdat_iso'),
            F.to_utc_timestamp(F.to_timestamp(F.col('json_data.lastModifiedAt')[0:19], patern_timestamp19_zulu),
                               time_zone_D).alias('lastmodifiedat_iso'),

            F.col('json_data.interimProductWish').alias('interimproductwish'),
            F.col('json_data.customerDetails.customerId').alias('tcomcustid'),

            F.col('json_data.customerDetails.telekomCustomerId').alias('telekomkundennummer_ps'),
            #F.lit(None).alias('telekomkundennummer_ps'),

            F.coalesce(
            F.col('json_data.location.buildingDetails.type').alias('buildingtype_0'),
            F.col('json_data.installationLocation.buildingDetails.type').alias('buildingtype_1')
            ).alias('buildingtype'),

            #F.col('json_data.location.buildingDetails.accommodationUnitAmount').alias('accommunit_0'),
            F.col('json_data.installationLocation.buildingDetails.accommodationUnitAmount').alias('accommunit'),


            #F.col('json_data.location.buildingDetails.floorAmount').alias('flooramount_0'),
            F.col('json_data.installationLocation.buildingDetails.floorAmount').alias('flooramount'),


            # not found this:
            #F.col('json_data.location.buildingDetails.businessUnitAmount').alias('businessunitamount_0'),
            #F.col('json_data.installationLocation.buildingDetails.businessUnitAmount').alias('businessunitamount_1'),
            F.lit(None).alias('businessunitamount'),

            F.coalesce(
            F.col('json_data.installationAddress.klsId').alias('klsid_ps_0'),
            F.col('json_data.installationLocation.address.klsId').alias('klsid_ps_1')
            ).alias('klsid_ps'),

            F.coalesce(
            F.col('json_data.installationAddress.klsValidated').alias('kls_validated_0'),
            F.col('json_data.installationLocation.address.klsValidated').alias('kls_validated_1')
            ).alias('kls_validated'),

            F.coalesce(
            F.col('json_data.installationAddress.street').alias('street_0'),
            F.col('json_data.installationLocation.address.street').alias('street_1')
            ).alias('street'),


            F.coalesce(
            F.col('json_data.installationAddress.city').alias('city_0'),
            F.col('json_data.installationLocation.address.city').alias('city_1')
            ).alias('city'),

            F.coalesce(
            F.col('json_data.installationAddress.zip').alias('zip_code_0'),
            F.col('json_data.installationLocation.address.zip').alias('zip_code_1')
            ).alias('zip_code'),

            #F.col('json_data.installationAddress.country').alias('country_0'),
            F.col('json_data.installationLocation.address.country').alias('country'),

            F.coalesce(
            F.col('json_data.salesDetails.channel').alias('saleschannel_0'),
            F.col('json_data.provisionData.channel').alias('saleschannel_1')
            ).alias('saleschannel'),

            #F.col('json_data.salesDetails.partner').alias('salespartner_0'),
            F.col('json_data.provisionData.salesPartner.partnerCode').alias('salespartner'),


            #not found this:
            #F.col('json_data.salesDetails.campaign').alias('salescampaign'), # salesDetails.campaign
            F.lit(None).alias('salescampaign'), # salesDetails.campaign

            F.coalesce(
            F.col('json_data.salesDetails.salesPointId').alias('salespointid_0'),
            F.col('json_data.provisionData.salesPointId').alias('salespointid_1')
            ).alias('salespointid'),

            F.coalesce(
            F.col('json_data.salesDetails.organisationId').alias('salesorganisationid_0'),
            F.col('json_data.provisionData.salesPartner.organisationId').alias('salesorganisationid_1')
            ).alias('salesorganisationid'),

            F.col('json_data.providerChange.portingAllNumbers').alias('portingallnumbers'),

            F.col('json_data.providerChange.carrierName').alias('carriername'),

            F.col('json_data.providerChange.carrierCode').alias('carriercode'),

            F.col('json_data.presalesContactAllowed').alias('presalescontactallowed'),
            F.col('json_data.businesscase').alias('businesscase'),

            F.col('json_data.customerInstallationDate').alias('customerinstallationdate'),

            F.col('json_data.customerInstallationOrderId').alias('customerinstallationorderid'),

            #  Boolean, CAST to True/False
            #F.when(F.col('json_data.location.landlordCompany') == None, False).otherwise(True).alias('landlordiscompany_0'),
            F.when(F.col('json_data.installationLocation.landlordCompany') == None, False).otherwise(True)
                .alias('landlordiscompany'),

            #F.col('json_data.location.landlordCompany.name').alias('companyname_0'),
            F.col('json_data.installationLocation.landlordCompany.name').alias('companyname'),

            #F.col('json_data.location.landlordCompany.legalForm').alias('legalform_0'),
            F.col('json_data.installationLocation.landlordCompany.legalForm').alias('legalform'),

            #F.col('json_data.location.landlordCompany.legalEntity').alias('legalentity_0'),
            F.col('json_data.installationLocation.landlordCompany.legalEntity').alias('legalentity'),
            #F.lit(None).alias('legalentity'),

            F.col('json_data.provisioningDetails.wishDate').alias('wishdate'),
            F.col('json_data.provisioningDetails.wishType').alias('wishtype'),

            # F.expr(), dtype will be:  array < struct.....  sourcedata for cl_f_presalesorder_orderitem_mt
            F.expr('json_data.items').alias('orderitems_array'),  # sourcedata for cl_f_presalesorder_orderitem_mt
            F.get_json_object('jsonstruct', '$.items').alias('orderitems_json'),

            F.expr('json_data.history').alias('history_array'),# sourcedata for cl_f_presalesorder_history_mt
            F.get_json_object('jsonstruct', '$.history').alias('history_json'),


            F.col('bdmp_loadstamp'),
            F.col('bdmp_id'),
            F.col('bdmp_area_id')


        )

        #df_al_json.show(5, False)
        #df_al_json.printSchema()

        # parse array  'orderitems_array' - json-explode do new dataframe
        df_pso_orderitems = self.parse_pso_orderitem_array(df_al_json)

        # parse array 'history_array' - json-explode do new dataframe
        df_pso_history = self.parse_pso_history_array(df_al_json)

        Func.bbe_process_log_table(self.spark_app.get_spark(),WF_AL2CL, self._etl_process_name,'INFO',
                                   'logic() finished','next step insert, table={0} '. \
                                   format(self._out_table_name),self._tmagic_messagetype)


        return  [df_al_json, df_pso_orderitems, df_pso_history]

    def handle_output_dfs(self, out_dfs_list: List):
        """
        Stores result data frames to output Hive tables
        """

        #return None   # skip INSERT,  debug only, devlab

        spark_io = util.ISparkIO.get_obj(self.spark_app.get_spark())

        # Read inputs
        df_pso = out_dfs_list[0]
        df_pso_orderitems = out_dfs_list[1]
        df_pso_history = out_dfs_list[2]
        doing_Insert = False

        # if dataframe doesn't have data - skip insert to table, no new data=no insert
        if df_pso:
            spark_io.df2hive(df_pso, DB_BBE_CORE, self._out_table_name , overwrite=False)
            doing_Insert = True
            Func.update_process_tracking_table(self.spark_app.get_spark(), self._etl_process_name, \
                                           self._in_table_name, self.max_acl_dop_val)

        Func.bbe_process_log_table(self.spark_app.get_spark(),WF_AL2CL, self._etl_process_name,'INFO',
                                   'end of process','insert table={0} ,doing_Insert={1}'. \
                                   format(self._out_table_name,doing_Insert),self._tmagic_messagetype)

        if df_pso_orderitems:
            spark_io.df2hive(df_pso_orderitems, DB_BBE_CORE, self._out_table_name_pso_orderitem, overwrite=False)
            Func.bbe_process_log_table(self.spark_app.get_spark(), WF_AL2CL, self._etl_process_name, 'INFO',
                                       'end of process-insert 2.', 'insert table={0} , '. \
                                       format(self._out_table_name_pso_orderitem), self._tmagic_messagetype)


        if df_pso_history:
            spark_io.df2hive(df_pso_history, DB_BBE_CORE, self._out_table_name_pso_history, overwrite=False)
            Func.bbe_process_log_table(self.spark_app.get_spark(), WF_AL2CL, self._etl_process_name, 'INFO',
                                       'end of process-insert 3.', 'insert table={0} , '. \
                                       format(self._out_table_name_pso_history), self._tmagic_messagetype)


        return df_pso



    # this function parsing/explode  order-items ARRAY , json_pso_orderItem
    def parse_pso_orderitem_array(self, df_in):

        # OK, no where filter
        df_json = df_in.select(df_in['acl_id_int'],
                               df_in['acl_dop_iso'],
                               df_in['acl_loadnumber_int'],
                               df_in['presalesorderid_ps'],
                               df_in['orderitems_array'])

        #df_json = df_in.filter((df_in['acl_id_int'] == 200655) | (df_in['acl_id_int'] == 200742))\
        #    .select(df_in['acl_id_int'], df_in['orderitems_struct'])


        df_json = df_json.withColumn('explod_pso_orderItem',F.explode("orderitems_array"))


        df_out = df_json.select(
            F.col('acl_id_int'),
            F.col('acl_dop_iso'),
            F.col('acl_loadnumber_int'),
            F.col('presalesorderid_ps'),
            F.col('explod_pso_orderItem.id').alias('orderitemid_ps'),
            F.col('explod_pso_orderItem.lineNumber').alias('lineNumber'),
            F.col('explod_pso_orderItem.productMaterialId').alias('productMaterialId'),
            F.col('explod_pso_orderItem.name').alias('productname'),
            F.col('explod_pso_orderItem.itemType').alias('orderitemtype'),
            F.lit(None).alias('bdmp_loadstamp'),
            F.lit(None).alias('bdmp_id'),
            F.lit(None).alias('bdmp_area_id')
        )

        #df_out.show(20,False)

        ####
        return df_out



    # this function parsing/explode  history ARRAY , json_pso_orderItem
    def parse_pso_history_array(self, df_in):

        # devlab test, 1x   acl_id = '200003137' --  20190513123845
        #  filter((df_in['acl_id_int'] == '200003137'))

        # OK, no where filter
        df_json = df_in.select(df_in['acl_id_int'],
                               df_in['acl_dop_iso'],
                               df_in['acl_loadnumber_int'],
                               df_in['presalesorderid_ps'],
                               df_in['history_array'])

        #df_json = df_in.filter((df_in['acl_id_int'] == 200655) | (df_in['acl_id_int'] == 200742))\
        #    .select(df_in['acl_id_int'], df_in['orderitems_struct'])


        df_json = df_json.withColumn('explod_pso_hist',F.explode("history_array"))


        df_out = df_json.select(
            F.col('acl_id_int'),
            F.col('acl_dop_iso'),
            F.col('acl_loadnumber_int'),
            F.col('presalesorderid_ps'),
            F.col('explod_pso_hist.id').alias('historyid_ps'),
            F.col('explod_pso_hist.itemType').alias('itemtype'),

            F.to_utc_timestamp(
                F.to_timestamp(
                F.col('explod_pso_hist.createdAt')[0:19], patern_timestamp19_zulu), time_zone_D)
                .alias('historyTimestamp_ISO'),

            # -- State transition
            F.col('explod_pso_hist.payload.data.from').alias('oldstate'),
            F.col('explod_pso_hist.payload.data.to').alias('newstate'),

            F.lit(None).alias('item_json'),
            F.lit(None).alias('jsonstruct'),

            F.lit(None).alias('bdmp_loadstamp'),
            F.lit(None).alias('bdmp_id'),
            F.lit(None).alias('bdmp_area_id')
        )

        #df_out.show(20,False)

        #  add FILTER:  'itemType' = 'state_transition'
        df_out = df_out.filter( df_out['itemtype'] == 'state_transition' )
        #df_out.show(10,False)

        ####
        return df_out
