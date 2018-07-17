# encoding: utf-8

"""

@author: kevin

@contact: kevin_678@126.com


@file: copy_hive_table_to_bigquery.py

@time: 2018/6/28 8:35

@desc:

"""
import argparse
import json

from config import config
from templet.spark_templet import SparkTemplet
from utils.big_query_utils import BigQueryConnector
from utils.execute_shell import ExecuteShell
from utils.logging_util import LogUtil

log = LogUtil()


class Hive2BigQuery(object):
    def __init__(self, args):
        self.parameter = args

    def get_cmd(self):
        options = json.dumps(self.parameter, default=lambda o: o.__dict__, sort_keys=True)
        return SparkTemplet(
            class_path="com.winfred.data.transform.Hive2BigQuery",
            run_jar_path=config.TASK_JAR_PATH,
            num_executors=4,
            executor_memory='2g',
            executor_cores=1,
            class_path_jars=config.SPARK_CLASSPATH_JARS_PATHS,
            options="'{}'".format(options.replace("\r", "")
                                  .replace("\n", "")
                                  .replace("\t", "")
                                  .replace(" ", "")
                                  )
        ).getSparkSubmitCmd()

    def run(self):
        cmd = self.get_cmd()
        print "start running cmd: {}".format(cmd)
        execute = ExecuteShell()
        (return_code, stdout, stderr) = execute.executeShell(cmd)
        log.info(stdout)
        log.info(stderr)
        return return_code

    def prepose_handler(self, parameter):
        sql_str = parameter.prepose_sql
        run_sql = False
        if sql_str is not None:
            run_sql = True
        elif parameter.target_database is not None \
                and parameter.target_table is not None \
                and parameter.partition_key is not None \
                and parameter.partition_value is not None:
            run_sql = True
            sql_str = "DELETE FROM {database_name}.{table_name} WHERE {partition_key}='{partition_value}';".format(
                database_name=parameter.target_database,
                table_name=parameter.target_table,
                partition_key=parameter.partition_key,
                partition_value=parameter.partition_value
            )
        if run_sql:
            big_query_connector = BigQueryConnector(sql_str)
            big_query_connector.exce_query()


class Parameter(object):
    def __init__(self,
                 source_database, source_table,
                 partition_key,
                 partition_value,
                 target_database, target_table,
                 target_date,
                 schema_column,
                 tmp_dir,
                 is_truncate,
                 prepose_sql
                 ):
        self.source_database = source_database
        self.source_table = source_table
        self.partition_key = partition_key
        self.partition_value = partition_value
        self.target_database = target_database
        self.target_table = target_table
        self.target_date = target_date
        self.schema_column = schema_column
        self.tmp_dir = tmp_dir
        self.project_id = config.BQ_PROJECT_ID
        self.bucket = config.BQ_BUCKET
        self.is_truncate = is_truncate
        self.prepose_sql = prepose_sql

    def __str__(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


def get_parse_args():
    # 获取参数
    parser = argparse.ArgumentParser(description="google cloud operation")
    parser.add_argument("-sd", "--source-database", help="source database", action="store",
                        type=str, default=None)
    parser.add_argument("-st", "--source-table", help="source table", action="store",
                        type=str, default=None)

    parser.add_argument("-pk", "--partition-key", help="partition key",
                        action="store",
                        type=str,
                        default=None)

    parser.add_argument("-pv", "--partition-value", help="partition value",
                        action="store",
                        type=str,
                        default=None)

    parser.add_argument("-td", "--target-database", help="target database", action="store",
                        type=str, default=None)
    parser.add_argument("-tt", "--target-table", help="target table", action="store", type=str,
                        default=None)

    parser.add_argument("-d", "--target-date", help="target date, FORMAT: yyyy-MM-dd", action="store", type=str,
                        default=None
                        )

    parser.add_argument("-sc", "--schema-column", help="bigquery table schema,  COLUMN1:TYPE1,COLUMN2:TYPE2",
                        action="store",
                        type=str,
                        default=None
                        )

    parser.add_argument("-tmd", "--tmp-dir", help="google storage tmp dir", action="store",
                        type=str,
                        default="bigquery_tmp"
                        )

    parser.add_argument("-it", "--is-truncate", help="truncate table", action="store",
                        type=bool,
                        default=False,
                        )
    # prepose
    parser.add_argument("-ps", "--prepose-sql", help="prepose sql", action="store",
                        type=str,
                        default=None,
                        )

    args = parser.parse_args()
    # 组装参数
    parameter_obj = Parameter(
        source_database=args.source_database,
        source_table=args.source_table,
        partition_key=args.partition_key,
        partition_value=args.partition_value,
        target_database=args.target_database,
        target_table=args.target_table,
        target_date=args.target_date,
        schema_column=args.schema_column,
        tmp_dir=args.tmp_dir,
        is_truncate=args.is_truncate,
        prepose_sql=args.prepose_sql
    )
    return parameter_obj


if __name__ == '__main__':
    parameter = get_parse_args()
    task_args = json.dumps(parameter, default=lambda o: o.__dict__, sort_keys=True, indent=4)
    print "parameter : {}".format(task_args)
    hive_to_bigquery = Hive2BigQuery(args=parameter)
    hive_to_bigquery.prepose_handler(parameter)
    exit(hive_to_bigquery.run())
