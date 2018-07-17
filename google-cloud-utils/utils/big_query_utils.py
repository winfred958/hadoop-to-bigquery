# encoding: utf-8

"""

@author: kevin

@contact: kevin_678@126.com


@file: big_query_utils.py

@time: 2018/7/5 19:11

@desc:

"""

from google.cloud import bigquery


class BigQueryConnector(object):
    def __init__(self, sql_str):
        self.sql_str = sql_str

    def exce_query(self):
        print self.sql_str
        bigquery_client = bigquery.Client()
        return bigquery_client.query(query=self.sql_str, location="US")


if __name__ == '__main__':
    sql_str = "DELETE FROM dw_ods.app_log_v1 WHERE dt = '2018-06-02';"
    bc = BigQueryConnector(sql_str=sql_str)
    bc.exce_query()
