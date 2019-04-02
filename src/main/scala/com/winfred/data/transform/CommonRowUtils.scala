package com.winfred.data.transform

import java.util

import com.google.gson.JsonObject
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField

object CommonRowUtils {

  def getJsonObj(row: Row): JsonObject = {
    val jsonObject = new JsonObject()

    val structFields: Array[StructField] = row.schema.fields
    val stringToFieldMap: util.Map[String, StructField] = getColumnStructMap(structFields)

    for (entry: util.Map.Entry[String, StructField] <- stringToFieldMap.entrySet()) {
      val columnName = entry.getKey
      val structField = entry.getValue
      val typeName = structField.dataType.typeName
      if (StringUtils.equalsAnyIgnoreCase(typeName, "string")) {
        jsonObject.addProperty(columnName, row.getAs[String](columnName))
      }


    }

    jsonObject
  }


  def getColumnStructMap(structFields: Array[StructField]): util.Map[String, StructField] = {
    val map = new util.HashMap[String, StructField]()
    structFields.foreach(field => {
      map.put(field.name, field)
    })
    map
  }
}
