#!/bin/bash

START_TIME=$1
END_TIME=$2

YEARSTERDAY_STR=$(date +"%Y-%m-%d" -d "-1 day")

if [[ "x${START_TIME}" = "x" ]]; then
    START_TIME=${YEARSTERDAY_STR}
fi


if [[ "x${END_TIME}" = "x" ]]; then
    END_TIME=$(date +"%Y-%m-%d")
fi

DATABASE_NAME=dw_ods
TABLE_NAME=app_log_v1
LOCATION_ROOT_PATH=hdfs://cdh5/user/pro/collect/cleaned/app_v1

HADOOP_TO_BIGQUERY_UTILS_HOME=/home/pro/google-cloud-utils

start_timestamp=$(date +%s -d ${START_TIME})
end_timestamp=$(date +%s -d ${END_TIME})

ALTER_CMD_STR=""
target_timestamp=${start_timestamp}
index=0
while ((${target_timestamp} < ${end_timestamp}))
do
    # 获取指定时间戳
    target_timestamp=`expr ${start_timestamp} + ${index} \* 86400`

    DATE_CMD="date --date=@${target_timestamp}"

    dt=$(${DATE_CMD} +'%Y-%m-%d')
    child_path=$(${DATE_CMD} +'%Y/%m/%d')

    year=$(${DATE_CMD} +'%Y')
    month=$(${DATE_CMD} +'%m')
    day=$(${DATE_CMD} +'%d')

    # echo -e "dt=${dt} \t\t child_path=${child_path}"

SYNC_CMD=$(cat <<!EOF
    ${HADOOP_TO_BIGQUERY_UTILS_HOME}/bin/hive2bigquery.sh \
     --source-database dw_ods \
     --source-table app_log_v1 \
     --partition-key dt \
     --partition-value ${dt} \
     --target-database dw_ods \
     --target-table app_log_v1 \
     --target-date ${dt} \
     --schema-column server_time:NUMERIC,server_datetime:TIMESTAMP,dt:DATE \
     --tmp-dir bigquery-tmp
!EOF)
    echo ${SYNC_CMD}
    index=`expr ${index} + 1`
done

echo -e $"\n${ALTER_CMD_STR}\n"

hive -e "${ALTER_CMD_STR}" -v

return_code=$?
exit return_code