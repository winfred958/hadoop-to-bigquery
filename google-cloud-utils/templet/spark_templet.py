# encoding: utf-8

"""

@author: kevin

@contact: kevin_678@126.com

@file: spark_templet.py

@time: 2017/6/16 9:10

@desc:

@Software: data-analysis

"""

from config import config


class SparkTemplet(object):
    def __init__(self,
                 class_path,
                 run_jar_path,
                 master="yarn",
                 deploy_mode="cluster",
                 driver_memory="4g",
                 num_executors=40,
                 executor_memory="2g",
                 executor_cores=1,
                 class_path_jars="",
                 options=""
                 ):
        """
        :param class_path: 类路径(主类)
        :param master: yarn/ mesos / local
        :param deploy_mode: client/ cluster
        :param driver_memory:
        :param executor_memory:
        :param executor_cores:
        :param num_executors:
        :param class_path_jars: 引用的外部jar
        :param run_jar_path: jar包路径
        :param options: jar参数
        """
        self.spark_home = config.SPARK_HOME
        self.class_path = class_path
        self.master = master
        self.deploy_mode = deploy_mode
        self.driver_memory = driver_memory
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.num_executors = num_executors
        self.hive_site_config_path = config.HIVE_SITE_CONFIG_PATH
        self.class_path_jars = class_path_jars
        self.run_jar_path = run_jar_path
        self.options = options

    def getSparkSubmitCmd(self):
        SPARK_TMPLET = """
            {spark_home}/bin/spark-submit \
            --class {class_path} \
            --master {master} \
            --deploy-mode {deploy_mode} \
            --driver-memory {driver_memory} \
            --num-executors {num_executors} \
            --executor-memory {executor_memory} \
            --executor-cores {executor_cores} \
            --conf spark.sql.warehouse.dir={spark_sql_warehouse_dir} \
            --files {hive_site_config_path} \
            --jars {class_path_jars} \
            {run_jar_path} {options}
            """.format(
            spark_home=config.SPARK_HOME,
            class_path=self.class_path,
            master=self.master,
            deploy_mode=self.deploy_mode,
            driver_memory=self.driver_memory,
            executor_memory=self.executor_memory,
            executor_cores=self.executor_cores,
            num_executors=self.num_executors,
            spark_sql_warehouse_dir=config.SPARK_SQL_WAREHOUSE_DIR,
            hive_site_config_path=self.hive_site_config_path,
            class_path_jars=config.SPARK_CLASSPATH_JARS_PATHS,
            run_jar_path=self.run_jar_path,
            options=self.options
        )
        return SPARK_TMPLET


if __name__ == '__main__':
    pass
