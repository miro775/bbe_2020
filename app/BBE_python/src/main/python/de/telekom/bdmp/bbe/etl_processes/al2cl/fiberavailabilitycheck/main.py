#!/usr/bin/python
# coding: utf-8


from de.telekom.bdmp.pyfw.etl_framework.sparkapp import SparkApp
from de.telekom.bdmp.pyfw.etl_framework.tree_etl import IProcessNode, TreeEtlRunner
from de.telekom.bdmp.bbe.etl_processes.al2cl.fiberavailabilitycheck.process import FACToClProcess


class FACProcessNode(IProcessNode):
    pass


if __name__ == '__main__':
    fac_to_cl_process = FACToClProcess()
    spark_app = SparkApp('Spark application for {0} process'.format(fac_to_cl_process.get_name()))
    fac_to_cl_process.set_spark_app(spark_app)
    fac_process_node = FACProcessNode(fac_to_cl_process)

    tree_etl_runner = TreeEtlRunner([fac_process_node])
    # if tree_etl_runner.run() == 3:
    #    raise Exception('RUN FAILED')
    tree_etl_runner.run()

    spark_app.stop()
