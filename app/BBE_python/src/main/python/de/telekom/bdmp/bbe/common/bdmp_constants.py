from de.telekom.bdmp.bdmf.base.environment import Environment

# determine only once the bdmp_env
# if not empty then insert an underline in front of it
my_bdmp_env = Environment().get('BDMP_ENV')     # my_bdmp_env = e.g. _prd or empty
my_bdmp_env_without_underscore = my_bdmp_env    # my_bdmp_env_without_underscore = e.g. prd
HDFS_BBE_PATH = '/bdmp/hive/{0}/'.format(my_bdmp_env_without_underscore)
if my_bdmp_env != "":
    my_bdmp_env = "_" + my_bdmp_env

DB_BBE_IN = 'db{0}_bbe_in_iws'.format(my_bdmp_env)
DB_BBE_CORE = 'db{0}_bbe_core_iws'.format(my_bdmp_env)
DB_BBE_BASE = 'db{0}_bbe_base_iws'.format(my_bdmp_env)

TABLE_BBE_PROCESS_TRACKING = 'cl_m_process_tracking_mt'
TABLE_BBE_PROCESS_LOG = 'cl_m_process_log_mt'

JOIN_LEFT_OUTER = 'left_outer'

WF_AL2CL = 'al2cl'  #  workflow name, for  log table
