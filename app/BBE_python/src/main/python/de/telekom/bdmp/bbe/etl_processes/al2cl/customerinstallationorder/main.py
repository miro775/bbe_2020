#!/usr/bin/python
# coding: utf-8

# CustomerInstallationOrder
from de.telekom.bdmp.pyfw.etl_framework.sparkapp import SparkApp
from de.telekom.bdmp.pyfw.etl_framework.tree_etl import IProcessNode, TreeEtlRunner
from de.telekom.bdmp.bbe.etl_processes.al2cl.customerinstallationorder.process import CIOToClProcess


class CIO_ProcessNode(IProcessNode):
    pass


if __name__ == '__main__':
    cio_to_cl_process = CIOToClProcess()
    spark_app = SparkApp('Spark application for {0} process'.format(cio_to_cl_process.get_name()))
    cio_to_cl_process.set_spark_app(spark_app)
    cio_process_node = CIO_ProcessNode(cio_to_cl_process)

    tree_etl_runner = TreeEtlRunner([cio_process_node])
    # if tree_etl_runner.run() == 3:
    #   raise Exception('RUN FAILED')
    tree_etl_runner.run()

    spark_app.stop()

