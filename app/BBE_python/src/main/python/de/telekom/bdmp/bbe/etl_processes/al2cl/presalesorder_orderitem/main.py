#!/usr/bin/python
# coding: utf-8

# CustomerInstallationOrder
from de.telekom.bdmp.pyfw.etl_framework.sparkapp import SparkApp
from de.telekom.bdmp.pyfw.etl_framework.tree_etl import IProcessNode, TreeEtlRunner
from de.telekom.bdmp.bbe.etl_processes.al2cl.presalesorder_orderitem.process import PsoItem_ToClProcess


class PSOi_ProcessNode(IProcessNode):
    pass


if __name__ == '__main__':
    pso_to_cl_process = PsoItem_ToClProcess()
    spark_app = SparkApp('Spark application for {0} process'.format(pso_to_cl_process.get_name()))
    pso_to_cl_process.set_spark_app(spark_app)
    pso_process_node = PSOi_ProcessNode(pso_to_cl_process)

    tree_etl_runner = TreeEtlRunner([pso_process_node])
    # if tree_etl_runner.run() == 3:
    #   raise Exception('RUN FAILED')
    tree_etl_runner.run()

    spark_app.stop()
