package com.winfred.data.transform

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * com.winfred.data.transform
  *
  * @author kevin
  * @since 2018/6/26 19:07
  */
object BigQuerySchemaTest {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")

    val sparkSession = SparkSession
      .builder()
      .appName("BigQuerySchemaTest")
      .config(sparkConf)
      .getOrCreate()

    import sparkSession.implicits._

    // DataFrame 模拟数据
    val sourceDS: DataFrame = sparkSession
      .createDataset(Seq(
        TestEntity(
          order_id = "aaaaaaaa",
          item_number = "11111111111",
          item_quantity = 10,
          item_sale_price = 0.55,
          update_timestamp = System.currentTimeMillis()
        ),
        TestEntity(
          order_id = "bbbbbbbbbbb",
          item_number = "22222222",
          item_quantity = 1,
          item_sale_price = 100.5,
          update_timestamp = System.currentTimeMillis()
        )
      ))
      .toDF()

    sourceDS
      .map(row => {
        val fields: Array[StructField] = row.schema.fields

        val resultList = new ListBuffer[SchemaEntity]
        for (i <- 0.until(fields.size)) {
          val structField = fields.apply(i)
          resultList += SchemaEntity(
            name = structField.name,
            typeName = structField.dataType.typeName
          )
        }

        for (e <- resultList) yield e
      })
      .flatMap(x => x)
      .show()

    sparkSession.close()

  }

  case class TestEntity(
                         order_id: String,
                         item_number: String,
                         item_quantity: Int,
                         item_sale_price: Double,
                         update_timestamp: Long
                       )

  case class SchemaEntity(
                           name: String,
                           typeName: String
                         )

}

