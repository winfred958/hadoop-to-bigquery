#!/bin/bash

PROJECT_ROOT_PATH=${1}

export PYTHONPATH=${PROJECT_ROOT_PATH}:$PYTHONPATH
export GOOGLE_APPLICATION_CREDENTIALS=/etc/google-cloud/conf/DP-f1ceb21088d5.json

SPARK_HOME=/opt/spark
SPARK_SQL_WAREHOUSE_DIR=hdfs://cdh5/user/pro/spark/warehouse

HOME_PATH=${1}

RUN_JAR_PATH=${HOME_PATH}/lib/hadoop-to-bigquery.jar
echo -e "=========================: BIGQUERY_UTILS_JAR=${RUN_JAR_PATH}"
PATH_MYSQL_CONNECTOR_JAR=/opt/cm/share/cmf/lib/mysql-connector-java.jar
