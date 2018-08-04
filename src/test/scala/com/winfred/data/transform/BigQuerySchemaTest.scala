package com.winfred.data.transform

import java.util

import com.alibaba.fastjson.JSON
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.gson.JsonObject
import com.winfred.data.transform.entity.BigQueryEntity

/**
  * com.winfred.data.transform
  *
  * @author kevin
  * @since 2018/6/26 19:07
  */
object BigQuerySchemaTest {


  def main(args: Array[String]): Unit = {

    val columns: Array[String] = Array("user_id", "dt")

    val outputTableSchemaJson: TableSchema = new TableSchema()
    val schemaList = new util.ArrayList[TableFieldSchema]()
    for (columnName <- columns) {
      val tableFieldSchema = new TableFieldSchema()
      tableFieldSchema.setName(columnName)
      tableFieldSchema.setType("STRING")
      schemaList.add(tableFieldSchema)
    }
    outputTableSchemaJson.setFields(schemaList)

    println(outputTableSchemaJson.getFields.toString)


    val jsonObject = new JsonObject()

    jsonObject.addProperty("a", "aasf")
    jsonObject.addProperty("b", "aasf")
    jsonObject.addProperty("c", "aasf")

    jsonObject.remove("a")


    val keyIt = jsonObject.keySet().iterator()

    while(keyIt.hasNext){
      val key  = keyIt.next()

      println(s"==${key} === ${jsonObject.get(key)}")
    }


  }
}

