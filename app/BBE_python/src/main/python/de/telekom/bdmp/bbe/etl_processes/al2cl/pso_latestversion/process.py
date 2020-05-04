
import de.telekom.bdmp.pyfw.etl_framework.util as util
from de.telekom.bdmp.pyfw.etl_framework.iprocess import IProcess
from de.telekom.bdmp.pyfw.etl_framework.dfcreator import DfCreator
from de.telekom.bdmp.bbe.common.bdmp_constants import WF_AL2CL, DB_BBE_BASE, DB_BBE_CORE, patern_timestamp19_zulu, time_zone_D

from de.telekom.bdmp.bbe.common.tmagic_json_paths import *
import de.telekom.bdmp.bbe.common.functions as Func

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from pyspark.sql.dataframe import DataFrame





class Pso_Latest_ToClProcess(IProcess):


    def __init__(self, save_dfs_if_exc=False, persist_result_dfs=False):
        """
        Constructor initialize the process

        """
        self._etl_process_name = 'proc_f_pso_latestversion'
        self._db_in = DB_BBE_BASE
        self._in_table_name = 'al_gigabit_message_mt'

        self._db_out = DB_BBE_CORE
        self._out_table_name = 'cl_f_presalesorder_latestversion_mt'


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
        Logic of the whole process,  PSO latest version
        """

        df_input = in_dfs


        # filter "PSO" only messages, only uprocessed records (alc_dop from : process-tracking-table)
        # for full-process AL2CL, disable filter:  & (df_input[tracked_col] > current_tracked_value)
        df_al = df_input.filter((df_input['messagetype'] == self._tmagic_messagetype) \
                                      & (df_input['Messageversion'] == '1'))


        # analyse JSON schema "read.json()" (struct) from all specific messages , filter  messagetype
        # json_schema_full=DataFrame, json_schema_full.schema' as StructType
        json_schema_full = self.spark_app.get_spark().read.json(df_al.rdd.map(lambda row: row.jsonstruct))
        #json_schema_full.printSchema()  # debug only


        self.new_records_count = df_al.count()

        # compute max value of acl_dop - needed for next transformation
        self.max_acl_dop_val = df_al.agg(F.max(df_al['acl_dop']).alias('max')).collect()[0][0]


        self.log.debug('### logic of process \'{0}\' started, all_PSO_records_count={1}'.\
                       format(self.name,self.new_records_count))

        Func.bbe_process_log_table(self.spark_app.get_spark(),WF_AL2CL, self._etl_process_name,'INFO',
                                   'logic of process started','all_PSO_records_count={0}'. \
                                   format(self.new_records_count),self._tmagic_messagetype)

        # parse data from JSON
        df_al_json = df_al.withColumn('json_data', F.from_json(F.col('jsonstruct'), json_schema_full.schema)) \
            .select(
            F.col('acl_id').alias('acl_id_int'),
            F.col('acl_dop').alias('acl_dop'),
            F.to_timestamp(F.col('acl_DOP'), 'yyyyMMddHHmmss').alias('acl_dop_ISO'),
            F.col('acl_loadnumber').alias('acl_loadnumber_int'),
            F.col('messagetype').alias('messagetype'),
            F.col('messageversion').alias('messageversion'),

            F.col('json_data.id').alias('presalesorderid_ps'),
            F.col('json_data.lastModifiedAt').alias('modification_date'),

            # truncate first 19chars like:  '2019-06-24T09:46:54'
            F.to_utc_timestamp(F.to_timestamp(F.col('json_data.lastModifiedAt')[0:19], patern_timestamp19_zulu),
                               time_zone_D).alias('lastmodifiedat_iso'),
            F.lit(None).alias('jsonstruct'),

        )

        #df_al_json.show(5, False)
        #df_al_json.printSchema()


        '''
        # logic from HQL:  IL2AL_PreSalesOrder_latest_version
        
        rankedEvents AS
        (
          select *, 
          ROW_NUMBER() OVER  (PARTITION BY preSalesOrderId_ps ORDER BY modification_date DESC, acl_ID_int desc, acl_DOP desc) 
          AS rnk
          from extractedEvents
        )
        select * from rankedEvents
        where rnk = 1
        
        
        '''

        # JSONSTRUCT is empty, OK
        df_al_json = df_al_json.select(
            F.col('acl_id_int'),
            F.col('acl_dop'),
            F.col('acl_dop_ISO'),
            F.col('acl_loadnumber_int'),
            F.col('messagetype'),
            F.col('messageversion'),
            F.col('presalesorderid_ps'),
            F.col('modification_date'),
            F.col('lastmodifiedat_iso'),
            F.col('jsonstruct'),
            F.row_number().over(
                Window.partitionBy("presalesorderid_ps").orderBy(F.col("lastmodifiedat_iso").desc(),
                                                                 F.col("acl_id_int").desc(),
                                                                 F.col("acl_dop").desc())
            ).alias("rnk"),
            F.lit(None).alias('bdmp_loadstamp'),
            F.lit(None).alias('bdmp_id'),
            F.lit(None).alias('bdmp_area_id')

        )

        df_al_json = df_al_json.filter(df_al_json['rnk'] == '1')

        return  df_al_json

    def handle_output_dfs(self, out_dfs):
        """
        Stores result data frames to output Hive tables
        """


        spark_io = util.ISparkIO.get_obj(self.spark_app.get_spark())

        # Read inputs
        df_pso = out_dfs

        #doing_Insert = False

        # if dataframe doesn't have data - skip insert to table, no new data=no insert
        if df_pso:
            spark_io.df2hive(df_pso, DB_BBE_CORE, self._out_table_name , overwrite=True)
            #doing_Insert = True
            Func.update_process_tracking_table(self.spark_app.get_spark(), self._etl_process_name, \
                                           self._in_table_name, self.max_acl_dop_val)

        Func.bbe_process_log_table(self.spark_app.get_spark(),WF_AL2CL, self._etl_process_name,'INFO',
                                   'end of process','insert table={0} ,overwrite'. \
                                   format(self._out_table_name),self._tmagic_messagetype)


        return df_pso

