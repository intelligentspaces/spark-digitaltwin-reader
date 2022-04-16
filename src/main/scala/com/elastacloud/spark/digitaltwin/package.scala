package com.elastacloud.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object digitaltwin {
  implicit class DigitalTwinDataFrameReader(val reader: DataFrameReader) {
    def digitaltwin(): DataFrame = reader.format("com.elastacloud.spark.digitaltwin").load()
  }
}
