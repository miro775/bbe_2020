#!/bin/bash
# ---------------------------------------------------------------------------
# BBE
# ---------------------------------------------------------------------------
#
# Description:   Additional bash functions for BBE_DATA
#
# ---------------------------------------------------------------------------

function get_bbe_data_classpath () {
# -----------------------------------------------------
# Description:	Print BBE Java classpath
# -----------------------------------------------------
	local CLASSPATH=`get_classpath`
	for JARFILE in `find ${BBE_JARS}/ -name "*.jar" -prune -type f`
	do
		CLASSPATH="${CLASSPATH}${JARFILE}:"
	done
	echo $CLASSPATH
}
