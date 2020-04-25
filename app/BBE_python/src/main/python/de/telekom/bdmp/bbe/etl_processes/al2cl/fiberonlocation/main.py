#!/usr/bin/python
# coding: utf-8

# CustomerInstallationOrder
from de.telekom.bdmp.pyfw.etl_framework.sparkapp import SparkApp
from de.telekom.bdmp.pyfw.etl_framework.tree_etl import IProcessNode, TreeEtlRunner
from de.telekom.bdmp.bbe.etl_processes.al2cl.fiberonlocation.process import FolToClProcess


class Fol_ProcessNode(IProcessNode):
    pass


if __name__ == '__main__':
    fol_to_cl_process = FolToClProcess()
    spark_app = SparkApp('Spark application for {0} process'.format(fol_to_cl_process.get_name()))
    fol_to_cl_process.set_spark_app(spark_app)
    fol_process_node = Fol_ProcessNode(fol_to_cl_process)

    tree_etl_runner = TreeEtlRunner([fol_process_node])
    # if tree_etl_runner.run() == 3:
    #   raise Exception('RUN FAILED')
    tree_etl_runner.run()

    spark_app.stop()

