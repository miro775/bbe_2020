#!/usr/bin/python
# coding: utf-8

 
from de.telekom.bdmp.pyfw.etl_framework.sparkapp import SparkApp
from de.telekom.bdmp.pyfw.etl_framework.tree_etl import IProcessNode, TreeEtlRunner
from de.telekom.bdmp.bbe.etl_processes.al2cl.process import TMagicToClProcess

#test only
from de.telekom.bdmp.bdmf.base.environment import Environment

class TMagicToClProcessNode(IProcessNode):
    pass


if __name__ == '__main__':

    tmagic_to_cl_process = TMagicToClProcess()
    spark_app = SparkApp('Spark application for {0} process'.format(tmagic_to_cl_process.get_name()))
    tmagic_to_cl_process.set_spark_app(spark_app)
    tmagic_process_node = TMagicToClProcessNode(tmagic_to_cl_process)

    tree_etl_runner = TreeEtlRunner([tmagic_process_node])
    tree_etl_runner.run()

    spark_app.stop()
