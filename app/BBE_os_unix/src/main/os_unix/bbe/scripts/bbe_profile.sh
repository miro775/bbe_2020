#!/bin/bash
# ---------------------------------------------------------------------------
# BBE_DATA
# ---------------------------------------------------------------------------
#
# Description:   Cluster independent bbe_data environment variables 
#
# ---------------------------------------------------------------------------

# ####################
# Include Guard
# ####################
[ -n "$BDMP_STREAM_BBE_DATA" ] && return || readonly BDMP_STREAM_BBE_DATA=1

# ####################
# Direct Dependencies
# ####################
# * BDMF must not be mentioned, it's always the first
# * Only the next level must be mentioned
# * Circular Dependencies are not allowed
# See https://tmv2175.devlab.de.tmo/foswiki/BDMP/EnvironmentVariables
# ####################

# bbe_data entry:
# source $BDMF_OS_UNIX/../otherbbe_data/otherbbe_data_profile.sh

# ####################
# General 
# ####################

# ####################
# NFS 
# ####################

export BBE_DATA_HBASE="${BDMP_APP}/hbase/bbe_data"
export BBE_DATA_HBASE_SCRIPTS="${BBE_DATA_HBASE}/scripts"
export BBE_DATA_HIVE="${BDMP_APP}/hive/bbe_data" 
export BBE_DATA_HIVE_HQL="${BBE_DATA_HIVE}/hql"
export BBE_DATA_HIVE_SCHEMA="${BBE_DATA_HIVE}/schema"
export BBE_DATA_JAVA="${BDMP_APP}/java/bbe_data"
export BBE_DATA_OOZIE="${BDMP_APP}/oozie/bbe_data"
export BBE_DATA_OOZIE_WORKFLOW="${BBE_DATA_OOZIE}/workflow"
export BBE_DATA_OS_UNIX="${BDMP_APP}/os_unix/bbe_data"
export BBE_DATA_SCRIPTS="${BBE_DATA_OS_UNIX}/scripts"
export BBE_DATA_TRANSFORM_SCRIPTS="${BDMP_APP}/udf_scripts/bbe_data"

# ####################
# Functions 
# ####################

# Source shell functions
if [[ -e ${BBE_DATA_SCRIPTS}/bbe_data_functions.sh ]]
then
	source ${BBE_DATA_SCRIPTS}/bbe_data_functions.sh
else 
	echo "bbe_data_functions.sh not found"
fi

# ####################
# HDFS 
# ####################

export BBE_DATA_HDFS_APP_HIVE="${BDMP_HDFS_APP}/hive/bbe_data"
export BBE_DATA_HDFS_APP_HIVE_SCHEMA_MANAGED="${BBE_DATA_HDFS_APP_HIVE}/schema/managed"
export BBE_DATA_HDFS_APP_OOZIE="${BDMP_HDFS_APP}/oozie/bbe_data"
export BBE_DATA_HDFS_APP_OOZIE_WORKFLOW="${BBE_DATA_HDFS_APP_OOZIE}/workflow"
export BBE_DATA_HDFS_APP_TMP="${BDMP_HDFS_APP}/tmp"
export BBE_DATA_HDFS_APP_HIVE_UDF="${BBE_DATA_HDFS_APP_HIVE}/udf"

# ####################
# Java 
# ####################

export BBE_DATA_JARS="${BBE_DATA_JAVA}/lib"
export BBE_DATA_JAVA_RESOURCES="${BBE_DATA_JAVA}/resources"
export BBE_DATA_CLASSPATH=`get_bbe_data_classpath`
