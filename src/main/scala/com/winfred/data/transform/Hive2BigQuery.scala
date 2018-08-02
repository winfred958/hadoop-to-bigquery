package com.winfred.data.transform

import java.text.SimpleDateFormat
import java.util
import java.util.Map.Entry
import java.util.{Calendar, TimeZone}

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, TypeReference}
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.hadoop.io.bigquery.output.{BigQueryOutputConfiguration, IndirectBigQueryOutputFormat}
import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, BigQueryFileFormat}
import com.google.gson.JsonObject
import com.winfred.data.transform.entity.BigQueryEntity
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * // projectId: bi-advice-204600
  * // buckerId: staging.bi-advice-204600.appspot.com
  */
object Hive2BigQuery {


  def main(args: Array[String]): Unit = {


    val args_str = if (null == args || args.isEmpty) null else args(0)
    if (StringUtils.isBlank(args_str)) {
      throw new IllegalArgumentException("参数有误, 请重新设置参数");
    }
    println(s"args: ${args_str}")
    var bigQueryEntity = new BigQueryEntity()
    bigQueryEntity = bigQueryEntity.getBigQueryEntity(args_str)

    println(s"======== parameter ↓ ========\n ${JSON.toJSONString(bigQueryEntity, 1, SerializerFeature.SortField)} \n ======== parameter ↑ ========")

    val sparkConf = new SparkConf();
    sparkConf.setMaster("yarn");
    sparkConf.setAppName("Hive2BigQuery");

    val sparkSession = SparkSession.builder().config(conf = sparkConf).enableHiveSupport().getOrCreate();

    syncTableToBigQuery(sparkSession = sparkSession, bigQueryEntity = bigQueryEntity)

    sparkSession.close()
  }

  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val simpleDateFormat_dir = new SimpleDateFormat("yyyy/MM/dd")

  def syncTableToBigQuery(sparkSession: SparkSession, bigQueryEntity: BigQueryEntity): Boolean = {

    if (null == bigQueryEntity) {
      throw new IllegalArgumentException("参数错误");
    }

    import sparkSession.sql

    val sourceDatabase = bigQueryEntity.getSourceDatabase
    val sourceTable = bigQueryEntity.getSourceTable

    val sourcePartitionKey = bigQueryEntity.getSourcePartitionKey
    val sourcePartitionValue = bigQueryEntity.getSourcePartitionValue

    if (StringUtils.isAnyBlank(sourceDatabase, sourceTable)) {
      throw new IllegalArgumentException("源表数据库表明参数错误");
    }

    var sourceDf: DataFrame = null

    if (StringUtils.isBlank(sourcePartitionKey) && StringUtils.isBlank(sourcePartitionValue)) {
      sourceDf = sql(
        s"""
           | SELECT
           |   *
           | FROM
           |   ${sourceDatabase}.${sourceTable}
       """.stripMargin)
    } else {
      sourceDf = sql(
        s"""
           | SELECT
           |   *
           | FROM
           |   ${sourceDatabase}.${sourceTable}
           | WHERE
           |   ${sourcePartitionKey} = '${sourcePartitionValue}'
       """.stripMargin)
    }
    syncDataFrameToBigQuery(sparkSession, sourceDf, bigQueryEntity)
  }

  def syncDataFrameToBigQuery(sparkSession: SparkSession, dataFrame: DataFrame, bigQueryEntity: BigQueryEntity): Boolean = {

    val targetDatabase = bigQueryEntity.getTargetDatabase
    val targetTable = bigQueryEntity.getTargetTable

    if (StringUtils.isAnyBlank(targetDatabase, targetTable)) {
      throw new IllegalArgumentException("目标表数据库表明参数错误");
    }

    val schemaStr = bigQueryEntity.getSchemaStr
    // yyyy/MM/dd 构造目录结构用
    val targetDate = bigQueryEntity.getTargetDate
    val dateDirStr = getDateFormatDir(targetDate)

    val columns: Array[String] = dataFrame.columns

    val schemaMapping: util.Map[String, String] = getSchemaMapping(columns = columns, schemaStr = schemaStr)

    val resultRdd = dataFrame.repartition(10).rdd.map(row => {
      val jsonObject = new JsonObject()
      for (columnName <- columns) {
        //        val value: String = String.valueOf(row.getAs[Any](columnName))

        val columnType: String = schemaMapping.get(columnName)

        if ("STRING".equalsIgnoreCase(columnType)) {
          val value: Any = row.getAs[Any](columnName)
          jsonObject.addProperty(columnName, if (null == value) null else String.valueOf(value))
        } else if ("INTEGER".equalsIgnoreCase(columnType)) {
          val value: AnyVal = row.getAs[AnyVal](columnName)
          jsonObject.addProperty(columnName, if (null == value) 0 else value.toString.toLong)
        } else if ("FLOAT".equalsIgnoreCase(columnType)) {
          val value: AnyVal = row.getAs[AnyVal](columnName)
          jsonObject.addProperty(columnName, if (null == value) 0.0 else value.toString.toDouble)
        } else if ("BOOLEAN".equalsIgnoreCase(columnType)) {
          val value: Boolean = row.getAs[Boolean](columnName)
          jsonObject.addProperty(columnName, value)
        } else {
          // 默认 String
          val value: Any = row.getAs[Any](columnName)
          jsonObject.addProperty(columnName, if (null == value) null else String.valueOf(value))
        }
      }
      // 删除 null value, bi
      val jsonKeyIt = jsonObject.keySet().iterator()
      while (jsonKeyIt.hasNext){
        val key = jsonKeyIt.next()
        if (jsonObject.get(key) == null) {
          jsonObject.remove(key)
        }
      }
      (null, jsonObject)
    })

    val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration

    val sparkContext = sparkSession.sparkContext

    val fullyQualifiedInputTableId = s"publicdata:${targetDatabase}.${targetTable}"


    val projectId = bigQueryEntity.getProjectId
    val bucket = bigQueryEntity.getBucket
    var tmpDir = bigQueryEntity.getTmpDir
    if (StringUtils.isBlank(tmpDir)) {
      tmpDir = "bigquery_tmp"
    }

    // Output parameters.
    val outputTableId = s"${projectId}:${targetDatabase}.${targetTable}"
    // Temp output bucket that is deleted upon completion of job.
    val outputGcsPath = (s"gs://${bucket}/${tmpDir}/${targetDatabase}/${targetTable}/${dateDirStr}")

    // 删除缓存目录
    Runtime.getRuntime.exec(s"hadoop fs -rm -r ${outputGcsPath}")

    println(s"outputGcsPath: ${outputGcsPath}")

    // Input configuration.
    hadoopConfiguration.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    hadoopConfiguration.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket)

    BigQueryConfiguration.configureBigQueryInput(hadoopConfiguration, fullyQualifiedInputTableId)

    val outputTableSchemaJson = new TableSchema()
    //    BigQueryConfiguration.configureBigQueryOutput()
    val schemaList: util.ArrayList[TableFieldSchema] = getSchemaList(columns = columns, schemaStr = schemaStr)
    outputTableSchemaJson.setFields(schemaList)
    println(s"======== schema ↓ ========\n${outputTableSchemaJson.getFields.toString}\n======== schema ↑ ========")

    // Output configuration.
    // Let BigQuery auto-detect output schema (set to null below).
    BigQueryOutputConfiguration.configure(
      hadoopConfiguration,
      outputTableId,
      outputTableSchemaJson,
      outputGcsPath,
      BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
      classOf[TextOutputFormat[_, _]]
    )

    hadoopConfiguration.set("mapreduce.job.outputformat.class",
      classOf[IndirectBigQueryOutputFormat[_, _]].getName)

    // Truncate the table before writing output to allow multiple runs.
    if (bigQueryEntity.getIs_truncate != null && bigQueryEntity.getIs_truncate) {
      hadoopConfiguration.set(BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY, "WRITE_TRUNCATE")
    } else {
      hadoopConfiguration.set(BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_KEY, BigQueryConfiguration.OUTPUT_TABLE_WRITE_DISPOSITION_DEFAULT)
    }

    // Overwrites (restates) a partition using the query results.
    hadoopConfiguration.set("configuration.query.writeDisposition", "WRITE_TRUNCATE")

    // Write data back into a new BigQuery table.
    // IndirectBigQueryOutputFormat discards keys, so set key to null.
    resultRdd.saveAsNewAPIHadoopDataset(hadoopConfiguration)

    true
  }


  /**
    * 获取 schema
    *
    * @param columns
    * @param schemaStr
    * @return
    */
  def getSchemaList(columns: Array[String], schemaStr: String): util.ArrayList[TableFieldSchema] = {
    val hashMap = new util.HashMap[String, TableFieldSchema]()
    val schemaList = new util.ArrayList[TableFieldSchema]()

    for (columnName <- columns) {
      val tableFieldSchema = new TableFieldSchema()
      tableFieldSchema.setName(columnName)
      tableFieldSchema.setType("STRING")
      hashMap.put(columnName, tableFieldSchema)
    }

    if (null != schemaStr && StringUtils.containsAny(schemaStr, ",", ":")) {
      val tmp = StringUtils.replaceAll(schemaStr, "\\s", "")
      tmp.split(",").foreach(filed_type => {
        val tableFieldSchema = new TableFieldSchema()
        if (StringUtils.contains(filed_type, ":")) {
          val ss = filed_type.split(":")
          val columnName = ss.apply(0)
          val typeStr = ss.apply(1)
          tableFieldSchema.setName(columnName)
          tableFieldSchema.setType(typeStr)
          hashMap.put(columnName, tableFieldSchema)
        }
      })
    }

    val set: util.Set[Entry[String, TableFieldSchema]] = hashMap.entrySet()
    val schemaIterator = set.iterator()
    while (schemaIterator.hasNext) {
      val entry = schemaIterator.next()
      schemaList.add(entry.getValue)
    }

    //    for ((k, v) <- hashMap) {
    //      schemaList.add(v)
    //    }

    return schemaList
  }

  /**
    *
    * yyyy-MM-dd 格式 转 yyyy/MM/dd
    * 构造日期层级目录使用
    *
    * @param dateStr
    * @return
    */
  def getDateFormatDir(dateStr: String): String = {
    if (StringUtils.isBlank(dateStr)) {
      // 默认当前日期
      return simpleDateFormat_dir.format(Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles")).getTime)
    }
    // 指定日期转化
    simpleDateFormat_dir.format(simpleDateFormat.parse(dateStr))
  }

  /**
    * 构建schema映射
    *
    * @param columns
    * @param schemaStr
    * @return
    */
  def getSchemaMapping(columns: Array[String], schemaStr: String): util.Map[String, String] = {
    val map: util.Map[String, String] = new util.HashMap[String, String]()
    // 默认string
    for (column <- columns) {
      map.put(column, "STRING")
    }
    if (StringUtils.isBlank(schemaStr)) {
      return map
    }
    // 参数 schema 覆盖默认值
    schemaStr.split(",").foreach(sch => {
      val k_v: Array[String] = sch.split(":")
      val key: String = k_v.apply(0).trim
      val value: String = k_v.apply(1).toUpperCase.trim
      map.put(key, value)
    })
    map
  }
}
