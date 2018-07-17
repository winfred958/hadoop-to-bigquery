# encoding: utf-8

"""

@author: kevin

@contact: kevin_678@126.com


@file: config.py

@time: 2018/6/28 9:26

@desc:

"""

LOG_DIR = "log/hive_to_bigquery.log"

BQ_PROJECT_ID = "bi-advice-xxxxxx"
BQ_BUCKET = "staging.bi-advice-xxxxxx.appspot.com"

# hadoop home
HADOOP_HOME = "/usr/bin"
SPARK_HOME = "/opt/spark"

HIVE_SITE_CONFIG_PATH = "/etc/hive/conf/hive-site.xml"
SPARK_CLASSPATH_JARS_PATHS = "/opt/cm/share/cmf/lib/mysql-connector-java.jar"
SPARK_SQL_WAREHOUSE_DIR = "hdfs://cdh5/user/pro/spark/warehouse"

TASK_JAR_PATH = "lib/hadoop-to-bigquery.jar"
