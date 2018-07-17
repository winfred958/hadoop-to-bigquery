package com.winfred.data.transform

import com.google.cloud.hadoop.io.bigquery.{BigQueryConfiguration, GsonBigQueryInputFormat}
import com.google.gson.JsonObject
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * com.winfred.data.transform
  *
  * @author kevin
  * @since 2018/6/23 18:19
  */
object ReadGoogleStorage {

  def main(args: Array[String]): Unit = {
    val databaseName = ""
    val tableName = ""

    val sparkConf = new SparkConf();
    sparkConf.setMaster("yarn");
    sparkConf.setAppName("ReadGoogleStorage");

    val sparkSession = SparkSession.builder().config(conf = sparkConf).enableHiveSupport().getOrCreate();

    import sparkSession.sql
    import sparkSession.implicits._

    val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration

    val sparkContext = sparkSession.sparkContext

    // Input parameters.
    val fullyQualifiedInputTableId = "publicdata:samples.shakespeare"
    val projectId = "bi-advice-204600"
    val bucket = "staging.bi-advice-204600.appspot.com"

    // Input configuration.
    hadoopConfiguration.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId)
    hadoopConfiguration.set(BigQueryConfiguration.GCS_BUCKET_KEY, bucket)

    BigQueryConfiguration.configureBigQueryInput(hadoopConfiguration, fullyQualifiedInputTableId)


    // Load data from BigQuery.
    val tableData = sparkContext.newAPIHadoopRDD(
      hadoopConfiguration,
      classOf[GsonBigQueryInputFormat],
      classOf[LongWritable],
      classOf[JsonObject]
    )

    // Perform word count.
    val wordCounts = tableData
      .map(entry => {
        convertToTuple(entry._2)
      })
      .reduceByKey(_ + _)

    // Display 10 results.
    wordCounts.take(10).foreach(l => println(l))

  }

  // Helper to convert JsonObjects to (word, count) tuples.
  def convertToTuple(record: JsonObject): (String, Long) = {
    val word = record.get("word").getAsString.toLowerCase
    val count = record.get("word_count").getAsLong
    return (word, count)
  }

  // Helper to convert (word, count) tuples to JsonObjects.
  def convertToJson(pair: (String, Long)): JsonObject = {
    val word = pair._1
    val count = pair._2
    val jsonObject = new JsonObject()
    jsonObject.addProperty("word", word)
    jsonObject.addProperty("word_count", count)
    return jsonObject
  }
}
