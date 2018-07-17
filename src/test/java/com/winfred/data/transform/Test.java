package com.winfred.data.transform;

import com.alibaba.fastjson.JSON;
import com.winfred.data.transform.entity.BigQueryEntity;

/**
 * @author kevin
 * @since 2018/6/29 13:53
 */
public class Test {

    public static void main(String[] args) {

        String str = "{\"bucket\":\"staging.bi-advice-204600.appspot.com\",\"partition_condition\":\"dt='2018-06-26'\",\"project_id\":\"bi-advice-204600\",\"schema_column\":\"server_time:NUMERIC,server_datetime:TIMESTAMP,dt:DATE\",\"source_database\":\"dw_ods\",\"source_table\":\"web_log_v1\",\"target_database\":\"dw_ods\",\"target_date\":\"2018-06-26\",\"target_table\":\"web_log_v1\",\"tmp_dir\":\"bigquery_tmp\"}";
        BigQueryEntity  bq = JSON.parseObject(str, BigQueryEntity.class);

        System.out.println(JSON.toJSONString(bq));

    }
}
