#!/bin/bash
# ---------------------------------------------------------------------------
# BBE_DATA
# ---------------------------------------------------------------------------
#
# Description:   BBE_DATA deployment script
#
# ---------------------------------------------------------------------------
set -o pipefail
set -x
CURRENT_VERSION=$1
TARGET_VERSION=$2
BDMP_STREAM=BBE_DATA
BDMP_STREAM_LOWER=bbe_data

if [ -z ${DEPLOY_STARTTIME} ] && [ -z ${DEPLOY_LOGDIRECTORY} ] 
then
	STARTTIME=`date +'%Y%m%d_%H%M%S'`
	LOGDIRECTORY=/var/log/bdmp/${BDMP_STREAM_LOWER}_${BDMP_ENV}_deploy_${STARTTIME}
else
	STARTTIME=${DEPLOY_STARTTIME}
	LOGDIRECTORY=${DEPLOY_LOGDIRECTORY}
fi
 
LOGFILE=${LOGDIRECTORY}/${BDMP_STREAM_LOWER}_${STARTTIME}.log
LOGFILE_NFS=${LOGDIRECTORY}/${BDMP_STREAM_LOWER}_nfs_${STARTTIME}.log
LOGFILE_CONFIG=${LOGDIRECTORY}/${BDMP_STREAM_LOWER}_config_${STARTTIME}.log
LOGFILE_TEMPLATE=${LOGDIRECTORY}/${BDMP_STREAM_LOWER}_template_${STARTTIME}.log
LOGFILE_HIVELIBS=${LOGDIRECTORY}/${BDMP_STREAM_LOWER}_hivelibs_${STARTTIME}.log
LOGFILE_HDFS=${LOGDIRECTORY}/${BDMP_STREAM_LOWER}_hdfs_${STARTTIME}.log
LOGFILE_SCHEMA2HDFS=${LOGDIRECTORY}/${BDMP_STREAM_LOWER}_schema2hdfs_${STARTTIME}.log
LOGFILE_HQL=${LOGDIRECTORY}/${BDMP_STREAM_LOWER}_hql_${STARTTIME}.log
LOGFILE_HBASE=${LOGDIRECTORY}/${BDMP_STREAM_LOWER}_hbase_${STARTTIME}.log
LOGFILE_UDFSCRIPT2HDFS=${LOGDIRECTORY}/${BDMP_STREAM_LOWER}_udfscript2hdfs_${STARTTIME}.log
 
mkdir -p ${LOGDIRECTORY}
 
python3 ${BDMP_PYTHON}/de/telekom/bdmp/bdmf/install/deploy.py -w linkconfigs -S BBE_DATA 2>&1 | tee -a ${LOGFILE} ${LOGFILE_CONFIG}
EXITCODE=${PIPESTATUS[0]}; if [ ${EXITCODE} -ne 0 ]; then echo Exiting linkconfigs with exitcode ${EXITCODE}; exit ${EXITCODE}; fi
python3 ${BDMP_PYTHON}/de/telekom/bdmp/bdmf/install/deploy.py -w unixfs -S BBE_DATA 2>&1 | tee -a ${LOGFILE} ${LOGFILE_NFS}
EXITCODE=${PIPESTATUS[0]}; if [ ${EXITCODE} -ne 0 ]; then echo Exiting unixfs with exitcode ${EXITCODE}; exit ${EXITCODE}; fi
python3 ${BDMP_PYTHON}/de/telekom/bdmp/bdmf/install/deploy.py -w linktemplates -S BBE_DATA 2>&1 | tee -a ${LOGFILE} ${LOGFILE_TEMPLATE}
EXITCODE=${PIPESTATUS[0]}; if [ ${EXITCODE} -ne 0 ]; then echo Exiting linktemplates with exitcode ${EXITCODE}; exit ${EXITCODE}; fi
python3 ${BDMP_PYTHON}/de/telekom/bdmp/bdmf/install/deploy.py -w linkhiveserverlibs -S $BDMP_STREAM 2>&1 | tee -a ${LOGFILE} ${LOGFILE_HIVELIBS}
EXITCODE=${PIPESTATUS[0]}; if [ ${EXITCODE} -ne 0 ]; then echo Exiting linkhiveserverlibs with exitcode ${EXITCODE}; exit ${EXITCODE}; fi
python3 ${BDMP_PYTHON}/de/telekom/bdmp/bdmf/install/deploy.py -w hdfs -S BBE_DATA 2>&1 | tee -a ${LOGFILE} ${LOGFILE_HDFS}
EXITCODE=${PIPESTATUS[0]}; if [ ${EXITCODE} -ne 0 ]; then echo Exiting hdfs with exitcode ${EXITCODE}; exit ${EXITCODE}; fi
python3 ${BDMP_PYTHON}/de/telekom/bdmp/bdmf/install/deploy.py -w avroschema2hdfs -S BBE_DATA 2>&1 | tee -a ${LOGFILE} ${LOGFILE_SCHEMA2HDFS}
EXITCODE=${PIPESTATUS[0]}; if [ ${EXITCODE} -ne 0 ]; then echo Exiting avroschema2hdfs with exitcode ${EXITCODE}; exit ${EXITCODE}; fi
python3 ${BDMP_PYTHON}/de/telekom/bdmp/bdmf/install/deploy.py -w udfscript2hdfs -S $BDMP_STREAM 2>&1 | tee -a ${LOGFILE} ${LOGFILE_UDFSCRIPT2HDFS}
EXITCODE=${PIPESTATUS[0]}; if [ ${EXITCODE} -ne 0 ]; then echo Exiting udfscript2hdfs with exitcode ${EXITCODE}; exit ${EXITCODE}; fi



hive_migration_script=${BBE_DATA_HIVE_HQL}/migrate/${BDMP_STREAM_LOWER}_migrate_v${CURRENT_VERSION}_v${TARGET_VERSION}.hql
hbase_migration_script=${BBE_DATA_HBASE_SCRIPTS}/migrate/${BDMP_STREAM_LOWER}_migrate_v${CURRENT_VERSION}_v${TARGET_VERSION}.sh

if [ -z ${CURRENT_VERSION} ] || [ -z ${TARGET_VERSION} ]
then
	echo 'Skipping HQL execution in snapshot deploy'
	echo 'Skipping HBase scripts execution in snapshot deploy'
else
    if [ -r "$hive_migration_script" ]; then
	    python3 ${BDMP_PYTHON}/de/telekom/bdmp/bdmf/install/deploy.py -w hqlscript -s $hive_migration_script -S $BDMP_STREAM 2>&1 | tee -a ${LOGFILE} ${LOGFILE_HQL}
	    EXITCODE=${PIPESTATUS[0]}; if [ ${EXITCODE} -ne 0 ]; then echo Exiting hqlscript with exitcode ${EXITCODE}; exit ${EXITCODE}; fi
	else
		echo "No Hive migration script ${CURRENT_VERSION} > ${TARGET_VERSION}";
	fi
	if [ -r "$hbase_migration_script" ]; then
	    . $hbase_migration_script | hbase shell -n 2>&1 | tee -a ${LOGFILE} ${LOGFILE_HBASE}
	    EXITCODE=${PIPESTATUS[0]}; if [ ${EXITCODE} -ne 0 ]; then echo Exiting HBase scripts with exitcode ${EXITCODE}; exit ${EXITCODE}; fi
	else
		echo "No HBase migration script ${CURRENT_VERSION} > ${TARGET_VERSION}";
	fi
fi