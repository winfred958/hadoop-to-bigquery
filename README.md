# 1. hadoop to google cloud deployment
## 1.1 git clone
  > * git clone https://github.com/winfred958/hadoop-to-bigquery.git
## 1.2 编译打包
  > * cd ./hadoop-to-bigquery
  > * mvn package
## 1.3 releases 压缩包放到deployment目录解压
  > * cp ./target/releases/hadoop-to-bigquery-*-hadoop2googlecloud.tar.gz ${DEPLOYMENT_DIR}/
  > * cd ${DEPLOYMENT_DIR}/
  > * tar -zxvf hadoop-to-bigquery-*-hadoop2googlecloud.tar.gz
## 1.4 修改配置
  > * vim ${DEPLOYMENT_DIR}/config/config.py ; 改为生产环境配置
## 1.5 部署完毕
  > * ${DEPLOYMENT_DIR}/bin/hive2bigquery.sh --help 查看用法
