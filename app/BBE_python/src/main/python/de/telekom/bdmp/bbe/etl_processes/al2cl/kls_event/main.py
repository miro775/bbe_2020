#!/usr/bin/python
# coding: utf-8

# CustomerInstallationOrder
from de.telekom.bdmp.pyfw.etl_framework.sparkapp import SparkApp
from de.telekom.bdmp.pyfw.etl_framework.tree_etl import IProcessNode, TreeEtlRunner
from de.telekom.bdmp.bbe.etl_processes.al2cl.kls_event.process import KlsToClProcess


class KLS_ProcessNode(IProcessNode):
    pass


if __name__ == '__main__':
    kls_to_cl_process = KlsToClProcess()
    spark_app = SparkApp('Spark application for {0} process'.format(kls_to_cl_process.get_name()))
    kls_to_cl_process.set_spark_app(spark_app)
    kls_process_node = KLS_ProcessNode(kls_to_cl_process)

    tree_etl_runner = TreeEtlRunner([kls_process_node])
    # if tree_etl_runner.run() == 3:
    #   raise Exception('RUN FAILED')
    tree_etl_runner.run()

    spark_app.stop()

