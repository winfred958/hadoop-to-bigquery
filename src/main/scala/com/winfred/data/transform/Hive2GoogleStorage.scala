package com.winfred.data.transform

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * com.winfred.data.transform
  *
  * @author kevin
  * @since 2018/6/26 16:18
  */
object Hive2GoogleStorage {
  def main(args: Array[String]): Unit = {
    val databaseName = "dw_ods"
    val tableName = "web_log_v1"

    val hiveTableDateCol = "dt"
    val dateStr = args.apply(0)

    val sparkConf = new SparkConf();
    sparkConf.setMaster("yarn");
    sparkConf.setAppName("Parquet2BigQuery");
    sparkConf.set("spark.sql.parquet.compression.codec", "uncompressed")

    val projectId = "bi-advice-204600"
    val bucket = "staging.bi-advice-204600.appspot.com"

//    val outputGcsPath = (s"gs://${bucket}/collect/cleaned/web_v1/${dateStr}/")

    val outputGcsPath = (s"gs://${bucket}/big_query_tmp/append_test")

    val sparkSession = SparkSession.builder().config(conf = sparkConf).enableHiveSupport().getOrCreate();

    import sparkSession.sql
    import sparkSession.implicits._

    //    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    val sourceDf: DataFrame = sql(
      s"""
         | SELECT
         |   *
         | FROM
         |  ${databaseName}.${tableName}
         | WHERE
         |  ${hiveTableDateCol} = '${dateStr}'
     """.stripMargin)

    sourceDf.repartition(3).coalesce(1).write.mode(SaveMode.Overwrite).json(outputGcsPath)

    sparkSession.close()
  }
}

