#!/usr/bin/python
# coding: utf-8


from de.telekom.bdmp.pyfw.etl_framework.sparkapp import SparkApp
from de.telekom.bdmp.pyfw.etl_framework.tree_etl import IProcessNode, TreeEtlRunner
from de.telekom.bdmp.bbe.etl_processes.al2cl.serviceorder_event.process import SOEToClProcess


class SOEventProcessNode(IProcessNode):
    pass


if __name__ == '__main__':
    soe_to_cl_process = SOEToClProcess()
    spark_app = SparkApp('Spark application for {0} process'.format(soe_to_cl_process.get_name()))
    soe_to_cl_process.set_spark_app(spark_app)
    soe_process_node = SOEventProcessNode(soe_to_cl_process)

    tree_etl_runner = TreeEtlRunner([soe_process_node])
    # if tree_etl_runner.run() == 3:
    #   raise Exception('RUN FAILED')
    tree_etl_runner.run()

    spark_app.stop()

