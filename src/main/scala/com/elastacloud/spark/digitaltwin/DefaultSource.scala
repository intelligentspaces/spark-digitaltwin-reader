package com.elastacloud.spark.digitaltwin

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class DefaultSource extends RelationProvider with SchemaRelationProvider with DataSourceRegister {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, None)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    createRelation(sqlContext, parameters, Some(schema))
  }

  private[digitaltwin] def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: Option[StructType]): BaseRelation = {
    val options = DigitalTwinClientOptions.from(parameters)
    DigitalTwinRelation(options, schema)(sqlContext)
  }

  override def shortName(): String = "digitaltwin"
}
