package com.elastacloud.spark.digitaltwin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

case class DigitalTwinRelation(options: DigitalTwinClientOptions, userSchema: Option[StructType] = None)(val sqlContext: SQLContext) extends BaseRelation with TableScan {
  val definedSchema: StructType = userSchema.getOrElse(inferSchema)

  override def schema: StructType = definedSchema

  private lazy val client = new DigitalTwinClient(options)

  override def buildScan(): RDD[Row] = {
    val rows = client.queryDigitalTwin(options.query, schema)
    val parsedRows = rows.map(resultsToRows)

    sqlContext.sparkContext.makeRDD(parsedRows).mapPartitions { iter =>
      iter.map { r => r }
    }
  }

  /**
   * Parses the returned results and converts them into [[Row]] objects. Where the results
   * contain nested data, these are also converted to allow the data to be parsed to a DataFrame
   *
   * @param row the row data to be parsed
   * @return values of the row transformed to a [[Row]] object
   */
  private def resultsToRows(row: Seq[Any]): Row = {
    Row.fromSeq(row.map {
      case seq: Seq[Any] => resultsToRows(seq)
      case v => v
    })
  }

  private def inferSchema: StructType = {
    // If the record contains a nested structure then return it as a JSON field
    // If the records do not contain nested structures then infer the schema from the records
    client.inferQuerySchema(options.query)
  }
}
