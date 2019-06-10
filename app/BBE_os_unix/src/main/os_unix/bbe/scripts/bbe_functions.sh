#!/bin/bash
# ---------------------------------------------------------------------------
# BBE_DATA
# ---------------------------------------------------------------------------
#
# Description:   Additional bash functions for BBE_DATA
#
# ---------------------------------------------------------------------------

function get_bbe_data_classpath () {
# -----------------------------------------------------
# Description:	Print BBE_DATA Java classpath
# -----------------------------------------------------
	local CLASSPATH=`get_classpath`
	for JARFILE in `find ${BBE_DATA_JARS}/ -name "*.jar" -prune -type f`
	do
		CLASSPATH="${CLASSPATH}${JARFILE}:"
	done
	echo $CLASSPATH
}
