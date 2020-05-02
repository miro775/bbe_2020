
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



class PsoItem_ToClProcess(IProcess):


    def __init__(self, save_dfs_if_exc=False, persist_result_dfs=False):
        """
        Constructor initialize the process

        """
        self._etl_process_name = 'proc_f_presalesorder_orderitem'
        self._db_in = DB_BBE_CORE
        self._in_table_name = 'cl_f_presalesorder_mt'

        self._db_out = DB_BBE_CORE
        self._out_table_name = 'cl_f_presalesorder_orderitem_mt'

        #self._tmagic_messagetype = -- not used,  this is CL2CL process
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
        df_these_messagetype_all = df_input
        json_schema_full = self.spark_app.get_spark().read.json(
            df_these_messagetype_all.rdd.map(lambda row: row.orderitems_struct))
        json_schema_full.printSchema()  # debug only


        # filter "PSO" only messages, only uprocessed records (alc_dop from : process-tracking-table)
        # for full-process AL2CL, disable filter:  & (df_input[tracked_col] > current_tracked_value)
        #df_al = df_input.filter( (df_input[tracked_col] > current_tracked_value) )

        df_al = df_input.filter( (df_input['acl_id_int'] == 200655) |  (df_input['acl_id_int'] == 200742) )


        json_schema_reduced = self.spark_app.get_spark().read.json(
            df_al.rdd.map(lambda row: row.orderitems_struct))
        json_schema_reduced.printSchema()  # debug only

        '''
        # reduced schema:

        root
         |-- conditions: array (nullable = true)
         |    |-- element: struct (containsNull = true)
         |    |    |-- conditionType: string (nullable = true)
         |    |    |-- id: long (nullable = true)
         |    |    |-- orderItemId: long (nullable = true)
         |    |    |-- performProcessing: boolean (nullable = true)
         |    |    |-- price: string (nullable = true)
         |    |    |-- telekomConditionId: string (nullable = true)
         |-- id: string (nullable = true)
         |-- itemType: string (nullable = true)
         |-- name: string (nullable = true)
         |-- orderId: string (nullable = true)
         |-- performProcessing: boolean (nullable = true)
         |-- pricingDate: string (nullable = true)
         |-- productMaterialId: string (nullable = true)

        '''



        #& ((df_input['acl_id'] == '5530944') | (df_input['acl_id'] == '5530907') | (df_input['acl_id'] == '200753')))  # 3rows for devlab debug



        self.new_records_count = df_al.count()

        # compute max value of acl_dop_ISO - needed for next transformation
        self.max_acl_dop_val = df_al.agg(F.max(df_al[tracked_col]).alias('max')).collect()[0][0]

        self.log.debug('### logic of process \'{0}\' started, current_tracked_value={1}, max_acl_dop={2}, new_records_count={3}'.\
                       format(self.name,current_tracked_value,self.max_acl_dop_val,self.new_records_count))

        Func.bbe_process_log_table(self.spark_app.get_spark(),WF_AL2CL, self._etl_process_name,'INFO',
                                   'logic of process started','current_tracked_value={0}, max_acl_dop={1}, new_records_count={2}'. \
                                   format(current_tracked_value, self.max_acl_dop_val,self.new_records_count))


        # IF DataFrame is empty , do not parse Json , no new data
        # "df_al.rdd.isEmpty()" ? - this can be performance problem ?!
        if self.new_records_count==0:
            self.log.debug('### logic of process \'{0}\' , input-dataFrame is empty, no new data'.format(self.name))
            return None




        #  df_al_json = df_al.withColumn('json_data', F.from_json(F.col('orderitems_struct'), json_schema_full.schema)) \
        #  df_al_json = df_al.withColumn('json_data', F.explode('orderitems_struct')) \

        #df_al_json = df_al.withColumn('json_data', F.explode('orderitems_struct')) \


        #df_al_json = df_al.withColumn('json_data', F.from_json(F.col('orderitems_struct'), json_schema_full.schema))

        #   select orderitems_struct from  db_d172_bbe_core_iws.cl_f_presalesorder_mt where acl_id_int =  200742 ;

        df_al.select(df_al['orderitems_struct']).show(2,True)

        '''  ??? [array]
        # df_al = df_input.filter( (df_input['acl_id_int'] == 200655) |  (df_input['acl_id_int'] == 200742) )
        +--------------------+
        |   orderitems_struct|
        +--------------------+
        |[{"id":"PoH{S","o...|
        |[{"id":"Pg3äS","o...|
        +--------------------+
        '''

        #  pyspark.sql.utils.AnalysisException: "cannot resolve 'explode(cl_f_presalesorder_mt.`orderitems_struct`)'
        #  due to data type mismatch: input to function explode should be array or map type, not string;

        # which schema?   json_schema_reduced.schema /  json_schema_full.schema
        df_al_json2 = df_al.withColumn('json_data', F.from_json(F.col('orderitems_struct'), json_schema_reduced.schema) )\
            .select("json_data.*")
        df_al_json2.show(2,False)
        # this return empty table, ONLY columns def.
        # ciastocny uspech, ked je iba jeden [] v array,tak to funguje pre ten jeden,,,,: inak asi nejak pouzit explode() ?
        # select  acl_id_int,orderitems_struct,  bdmp_area_id from  db_d172_bbe_core_iws.cl_f_presalesorder_mt where acl_id_int in ( 200655, 200742) ; 



        # bdmp_area_id
        df_al_json3 = df_al.withColumn('json_data', F.explode('orderitems_struct')).select("json_data.*")
        df_al_json3.show()
        df_al_json3.printSchema()




        df_al_json = df_al.withColumn('json_data', F.from_json(F.col('orderitems_struct'), json_schema_full.schema))\
            .select(
            F.col('acl_id_int'),
            F.col('acl_dop_ISO'),
            F.col('acl_loadnumber_int'),

            F.col('presalesorderid_ps'),

            F.expr("json_data.conditions").alias('orderitems_id'),
            #F.get_json_object('orderitems_struct',"$.id").alias("full_jsn"),
            #F.col('json_explod.id').alias('orderitems_id'),
            #F.col('json_explod.productMaterialId').alias('orderitems_productMaterialId'),
            #F.col('json_explod.name').alias('orderitems_name'),
            #F.col('json_explod.itemType').alias('orderitems_itemType'),

            #F.col('bdmp_loadstamp'),
            #F.col('bdmp_id'),
            #F.col('bdmp_area_id')

        )

        df_al_json.show(1, False)
        #df_al_json.printSchema()






        return  df_al_json

    def handle_output_dfs(self, out_dfs):
        """
        Stores result data frames to output Hive tables
        """

        return None   # skip INSERT,  debug only devlab

        spark_io = util.ISparkIO.get_obj(self.spark_app.get_spark())

        # Read inputs
        df_cl_tmagic = out_dfs
        doing_Insert = False

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
