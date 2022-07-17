/*
 * Copyright 2022 Elastacloud Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.elastacloud.spark.digitaltwin

import com.elastacloud.spark.digitaltwin.DigitalTwinRelation.{testDefinedData, testDefinedSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class DigitalTwinRelation(options: DigitalTwinClientOptions, userSchema: Option[StructType] = None)(val sqlContext: SQLContext) extends BaseRelation with TableScan {
  override def schema: StructType = userSchema.getOrElse(testDefinedSchema)

  override def buildScan(): RDD[Row] = {
    sqlContext.sparkContext.makeRDD(testDefinedData).mapPartitions(iter => iter.map { r => Row.fromSeq(r) })
  }
}

object DigitalTwinRelation {
  var testDefinedSchema: StructType = _
  var testDefinedData: Seq[Seq[Any]] = _
}

class DefaultSourceTests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val options = Map[String, String](
    "tenantId" -> "the-tenant-id",
    "clientId" -> "some-client-id",
    "clientSecret" -> "super-secret-value",
    "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
    "query" -> "SELECT * FROM DIGITALTWINS"
  )

  private lazy val spark = SparkSession.builder
    .appName("DefaultSourceTests")
    .master("local[2]")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.close()
    SparkSession.clearDefaultSession()
    SparkSession.clearActiveSession()
  }

  "Querying using a digital twin" should "allow the short name to be used" in {
    DigitalTwinRelation.testDefinedSchema = StructType(Array(StructField("col1", StringType, nullable = true)))
    DigitalTwinRelation.testDefinedData = Seq(Seq("a"), Seq("b"), Seq("c"))

    val df = spark.read.format("digitaltwin").options(options).load()
    val rows = df.collect()

    rows.length should be(3)
  }

  it should "allow the fully qualified type name to be used" in {
    DigitalTwinRelation.testDefinedSchema = StructType(Array(StructField("col1", StringType, nullable = true)))
    DigitalTwinRelation.testDefinedData = Seq(Seq("a"), Seq("b"), Seq("c"))

    val df = spark.read.format("com.elastacloud.spark.digitaltwin").options(options).load()
    val rows = df.collect()

    rows.length should be(3)
  }

  it should "allow the package provided name to be used" in {
    DigitalTwinRelation.testDefinedSchema = StructType(Array(StructField("col1", StringType, nullable = true)))
    DigitalTwinRelation.testDefinedData = Seq(Seq("a"), Seq("b"), Seq("c"))

    val df = spark.read.options(options).digitaltwin()
    val rows = df.collect()

    rows.length should be(3)
  }

  "Schemas" should "be inferred if not provided" in {
    DigitalTwinRelation.testDefinedSchema = StructType(Array(StructField("col1", StringType, nullable = true)))
    DigitalTwinRelation.testDefinedData = Seq(Seq("a"), Seq("b"), Seq("c"))

    val df = spark.read.options(options).digitaltwin()

    df.schema should contain theSameElementsAs StructType(Array(StructField("col1", StringType, nullable = true)))
  }

  it should "use the user defined schema if provided" in {
    // Invalid schema if it's inferred
    DigitalTwinRelation.testDefinedSchema = StructType(Array(StructField("col1", StringType, nullable = true)))

    // Valid schema for user defined
    val userDefinedSchema = StructType(Array(StructField("col2", IntegerType, nullable = true)))

    DigitalTwinRelation.testDefinedData = Seq(Seq(1), Seq(2), Seq(3))

    val df = spark.read.options(options).schema(userDefinedSchema).digitaltwin()

    df.schema should contain theSameElementsAs userDefinedSchema
  }
}
