#!/bin/bash
# ---------------------------------------------------------------------------
# BBE
# ---------------------------------------------------------------------------
#
# Description:   Additional bash functions for BBE_DATA
#
# ---------------------------------------------------------------------------
# wrong name [get_bbe_data_classpath]  fixed to [get_bbe_classpath] , 30.3.2020

function get_bbe_classpath () {
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
