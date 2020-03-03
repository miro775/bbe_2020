
import de.telekom.bdmp.pyfw.etl_framework.util as util
from de.telekom.bdmp.pyfw.etl_framework.iprocess import IProcess
from de.telekom.bdmp.pyfw.etl_framework.dfcreator import DfCreator
from de.telekom.bdmp.bbe.common.bdmp_constants import DB_BBE_BASE, DB_BBE_CORE

class TMagicToClProcess(IProcess):
    """
    Bestand process
    """

    def __init__(self, save_dfs_if_exc=False, persist_result_dfs=False):
        """
        Constructor initialize the process
        """

        IProcess.__init__(self, 'TMagic', DB_BBE_BASE + ' - al tables', DB_BBE_CORE + ' - cl tables',
                          save_dfs_if_exc=save_dfs_if_exc, persist_result_dfs=persist_result_dfs)

    def prepare_input_dfs(self, in_dfs):
        """
        Preparation of input data frames
        """

        # Df creator class
        df_creator = DfCreator(self.spark_app.get_spark())

        # DWHM
        #df_al_d_dwhm_bestand_ps = df_creator.get_df(database=DB_BBE_BASE,  table='al_d_dwhm_bestand_ps_mt')
        #df_al_d_dwhm_push_ps = df_creator.get_df(database=DB_BBE_BASE,  table='al_d_dwhm_push_ps_mt')

        #return [df_al_d_dwhm_bestand_ps, df_al_d_dwhm_push_ps]

    def logic(self, in_dfs):
        """
        Logic of the whole process
        """

        df_al_d_dwhm_bestand_ps, df_al_d_dwhm_push_ps = in_dfs

        #common_transform = CommonTransform()

        #df_cl_d_dwhm_bestand_ps = common_transform.trans_al2cl(df_al_d_dwhm_bestand_ps)
        #df_cl_d_dwhm_push_ps = common_transform.trans_al2cl(df_al_d_dwhm_push_ps)

        #return [df_cl_d_dwhm_bestand_ps, df_cl_d_dwhm_push_ps]

    def handle_output_dfs(self, out_dfs):
        """
        Stores result data frames to output Hive tables
        """

        spark_io = util.ISparkIO.get_obj(self.spark_app.get_spark())

        # Read inputs
        df_cl_d_dwhm_bestand_ps, df_cl_d_dwhm_push_ps = out_dfs

        spark_io.df2hive(df_cl_d_dwhm_bestand_ps, DB_BBE_CORE, 'cl_tmagic_mt', overwrite=True)
        #spark_io.df2hive(df_cl_d_dwhm_push_ps, DB_BBE_CORE, 'cl_d_dwhm_push_ps_mt', overwrite=True)

        return [df_cl_d_dwhm_bestand_ps, df_cl_d_dwhm_push_ps]

