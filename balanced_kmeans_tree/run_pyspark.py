import findspark
import getpass
import os
import platform
import py4j
import sys
import time
import random


class PySparkMgr():
    def __init__(self, args):
        self.__sc = None
        self.__appName = None
        self.__args = args

    def start(self, user):
        epochTime = int(time.mktime(time.localtime()))
        randomNum = random.randint(0, 10000)
        self.__appName = user + "-pyspark-" + str(epochTime) + "-" + str(randomNum)

        value = self.__args.get('spark.home')
        if not value:
            value = "/data/software/spark-2.3.1-bin-hadoop2.7"

        findspark.init(spark_home = value, extra_env_file = "/etc/dw-env.sh", python_path = "python3")

        value = self.__args.get('spark.driver.memory')
        if not value:
            value = "3g"
        findspark.set_driver_mem(value)

        value = self.__args.get('spark.executor.memory')
        if not value:
            value = "3g"
        findspark.set_executor_mem(value)

        findspark.set_app_name(self.__appName)
        findspark.end()

        import pyspark
        from pyspark import SparkConf
        from pyspark.sql import SparkSession, SQLContext

        from pyspark.context import SparkContext
        if os.environ.get("SPARK_EXECUTOR_URI"):
            SparkContext.setSystemProperty("spark.executor.uri", os.environ["SPARK_EXECUTOR_URI"])

        SparkContext._ensure_initialized()
        pySpark = None

        sc_conf = SparkConf()
        sc_conf.set('spark.locality.wait', 30000)
        sc_conf.set('spark.sql.autoBroadcastJoinThreshold', -1)
        sc_conf.set('spark.scheduler.minRegisteredResourcesRatio', 1)

        value = self.__args.get('spark.executor.cores')
        if not value:
           value = '1'
        sc_conf.set('spark.executor.cores', int(value))

        value = self.__args.get('spark.executor.instances')
        if not value:
           value = '1'
        sc_conf.set('spark.executor.instances', int(value))

        try:
            # Try to access HiveConf, it will raise exception if Hive is not added
            SparkContext._jvm.org.apache.hadoop.hive.conf.HiveConf()
            spark = SparkSession.builder.enableHiveSupport().config(conf = sc_conf).getOrCreate()

            from py4j.java_gateway import java_import
            from pyspark.context import SparkContext
            gw = SparkContext._gateway
            java_import(gw.jvm, "org.apache.spark.sql.TiSparkSession")
            pySpark = gw.jvm.TiSparkSession.builder().getOrCreate()
        except py4j.protocol.Py4JError:
            spark = SparkSession.builder.config(conf = sc_conf).getOrCreate()
        except TypeError:
            spark = SparkSession.builder.config(conf = sc_conf).getOrCreate()

        sc = spark.sparkContext
        sc.setJobGroup("", "Started By : {}".format(user), False)

        return pySpark if pySpark else spark, spark, sc

if __name__ == "__main__":
    spark_args = {}

    pysparkmgr = PySparkMgr(spark_args)
    _, spark, sc = pysparkmgr.start('xubin.xu')
    print(sc)
