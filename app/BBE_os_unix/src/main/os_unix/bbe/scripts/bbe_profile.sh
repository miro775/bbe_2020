#!/bin/bash
# ---------------------------------------------------------------------------
# BBE
# ---------------------------------------------------------------------------
#
# Description:   Cluster independent bbe environment variables 
#
# ---------------------------------------------------------------------------

# ####################
# Include Guard
# ####################
[ -n "$BDMP_STREAM_BBE" ] && return || readonly BDMP_STREAM_BBE=1

# ####################
# Direct Dependencies
# ####################
# * BDMF must not be mentioned, it's always the first
# * Only the next level must be mentioned
# * Circular Dependencies are not allowed
# See https://tmv2175.devlab.de.tmo/foswiki/BDMP/EnvironmentVariables
# ####################

# bbe entry:
# source $BDMF_OS_UNIX/../otherbbe/otherbbe_profile.sh

# ####################
# General 
# ####################

# ####################
# NFS 
# ####################

export BBE_HBASE="${BDMP_APP}/hbase/bbe"
export BBE_HBASE_SCRIPTS="${BBE_HBASE}/scripts"
export BBE_HIVE="${BDMP_APP}/hive/bbe" 
export BBE_HIVE_HQL="${BBE_HIVE}/hql"
export BBE_HIVE_SCHEMA="${BBE_HIVE}/schema"
export BBE_JAVA="${BDMP_APP}/java/bbe"
export BBE_OOZIE="${BDMP_APP}/oozie/bbe"
export BBE_OOZIE_WORKFLOW="${BBE_OOZIE}/workflow"
export BBE_OS_UNIX="${BDMP_APP}/os_unix/bbe"
export BBE_SCRIPTS="${BBE_OS_UNIX}/scripts"
# export BBE_TRANSFORM_SCRIPTS="${BDMP_APP}/udf_scripts/bbe"

# ####################
# Functions 
# ####################

# Source shell functions
if [[ -e ${BBE_SCRIPTS}/bbe_functions.sh ]]
then
	source ${BBE_SCRIPTS}/bbe_functions.sh
else 
	echo "bbe_functions.sh not found"
fi

# ####################
# HDFS 
# ####################

export BBE_HDFS_APP_HIVE="${BDMP_HDFS_APP}/hive/bbe"
export BBE_HDFS_APP_HIVE_SCHEMA_MANAGED="${BBE_HDFS_APP_HIVE}/schema/managed"
export BBE_HDFS_APP_OOZIE="${BDMP_HDFS_APP}/oozie/bbe"
export BBE_HDFS_APP_OOZIE_WORKFLOW="${BBE_HDFS_APP_OOZIE}/workflow"
export BBE_HDFS_APP_TMP="${BDMP_HDFS_APP}/tmp"
# export BBE_HDFS_APP_HIVE_UDF="${BBE_HDFS_APP_HIVE}/udf"

# ####################
# Java 
# ####################

export BBE_JARS="${BBE_JAVA}/lib"
export BBE_JAVA_RESOURCES="${BBE_JAVA}/resources"
# export BBE_CLASSPATH=`get_bbe_classpath` # disabled 28.11.2019, miro
