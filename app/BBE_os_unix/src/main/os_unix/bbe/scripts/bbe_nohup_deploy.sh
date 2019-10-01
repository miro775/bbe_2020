#!/bin/bash

STARTTIME=`date +'%Y%m%d_%H%M%S'`
LOGDIRECTORY=/var/log/bdmp/bbe_${BDMP_ENV}_deploy_${STARTTIME}
LOGFILE=${LOGDIRECTORY}/bbe_${STARTTIME}.log
CURRENT_VERSION=$1
TARGET_VERSION=$2

DEPLOY_STARTTIME=${STARTTIME} DEPLOY_LOGDIRECTORY=${LOGDIRECTORY} nohup bash ${BDMF_SCRIPTS}/bbe_data_deploy.sh ${CURRENT_VERSION} ${TARGET_VERSION} 2>&1 >/dev/null &

echo 'You can monitor the process with'
echo "less +F ${LOGFILE}"

