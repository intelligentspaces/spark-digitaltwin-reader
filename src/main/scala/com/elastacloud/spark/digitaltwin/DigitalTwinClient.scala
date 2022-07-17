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

import com.elastacloud.spark.digitaltwin.auth.AccessToken
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.util.EntityUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, JValue}

import java.net.URI
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, OffsetDateTime, ZoneOffset, ZonedDateTime}
import scala.util.Try

private[digitaltwin] class DigitalTwinClient(options: DigitalTwinClientOptions) extends Logging {
  private val digitalTwinResourceId = "0b07f429-9f4b-4714-9392-cc5e8e80c8b0"
  private val apiVersion = "2020-10-31"
  private val queryColumnRegex = """(?is)SELECT\s+(.*)\sFROM\s+(.+)$""".r

  private var authentication: AccessToken = _

  /**
   * Collection of ISO formats for timestamps
   */
  private val timestampFormats = Vector(
    DateTimeFormatter.ISO_DATE_TIME,
    DateTimeFormatter.ISO_OFFSET_DATE_TIME,
    DateTimeFormatter.ISO_LOCAL_DATE_TIME,
    DateTimeFormatter.ISO_ZONED_DATE_TIME)

  /**
   * Gets the [[URI]] for the Query API end-point
   *
   * @return the [[URI]] of the Query API end-point
   */
  private def getQueryEndpoint: URI = {
    new URI(options.endpoint).resolve(s"/query?api-version=$apiVersion")
  }

  /**
   * Gets the HTTP client. Implemented as protected so it can be overridden in the unit tests.
   *
   * @return a closeable HTTP client
   */
  protected def getHttpClient: CloseableHttpClient = {
    HttpClientBuilder.create.build
  }

  implicit val formats: DefaultFormats.type = DefaultFormats

  /**
   * Gets a bearer token to use in querying the digital twin
   *
   * @return the resource scoped bearer token [[String]] for the provided user
   */
  protected def getBearerToken: String = {
    logDebug("Getting bearer token")

    if (authentication == null || authentication.expires_on_unix < OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond) {
      logInfo("Current authentication token does not exist or has expired, refreshing")

      val formParameters = Seq(
        s"client_id=${options.clientId}",
        s"client_secret=${options.clientSecret}",
        s"resource=$digitalTwinResourceId",
        "grant_type=client_credentials"
      ).mkString("&")

      val authUri = s"https://login.microsoftonline.com/${options.tenantId}/oauth2/token"

      val request = new HttpPost(authUri)
      request.setHeader("Content-Type", "application/x-www-form-urlencoded")
      request.setHeader("Accept", "application/json")
      request.setEntity(new StringEntity(formParameters))

      val client = getHttpClient
      val authResponse = client.execute(request)

      if (authResponse.getStatusLine.getStatusCode != 200) {
        throw DigitalTwinClientException(s"Unable to acquire client token: ${authResponse.getStatusLine.getStatusCode} - ${authResponse.getStatusLine.getReasonPhrase}")
      }

      logInfo("Successfully retrieved authentication token digital twin")
      val authResults = parse(EntityUtils.toString(authResponse.getEntity)).extract[AccessToken]

      authentication = authResults
    }

    authentication.access_token
  }

  /**
   * Executes a query against the digital twin data plane
   *
   * @param client            the HTTP client to use for the query
   * @param query             string to use to query the twin
   * @param continuationToken if provided this will retrieve the next page of results for an existing query
   * @return A tuple of [[JValue]] for the retrieved results, and a continuation token if one is available
   */
  private def executeQuery(client: CloseableHttpClient, query: String, continuationToken: Option[String]): (JValue, Option[String]) = {
    val requestEntity = new StringEntity(
      continuationToken match {
        case Some(ct) => s"""{ "continuationToken": "${ct.replaceAll("\"", """\\\"""")}" }"""
        case None => s"""{ "query": "$query" }"""
      }
    )

    val token = getBearerToken

    val request = new HttpPost(getQueryEndpoint)
    request.setHeader("Authorization", s"Bearer $token")
    request.setHeader("Content-Type", "application/json")
    request.setHeader("max-items-per-page", "100")
    request.setEntity(requestEntity)

    logInfo(s"Executing query against digital twin${if (continuationToken.nonEmpty) " with continuation token"}: $query")
    val response = client.execute(request)

    if (response.getStatusLine.getStatusCode != 200) {
      val fullMessage = EntityUtils.toString(response.getEntity)
      throw DigitalTwinClientException(s"Digital twin returned an unsuccessful status code: ${response.getStatusLine.getStatusCode} - ${response.getStatusLine.getReasonPhrase}\n$fullMessage")
    }

    logInfo("Successfully received data from digital twin")
    val responseEntity = parse(EntityUtils.toString(response.getEntity))

    (
      responseEntity \ "value",
      (responseEntity \ "continuationToken").extract[Option[String]]
    )
  }

  /**
   * Queries the digital twin to retrieve data with a given structure
   *
   * @param query  Digital Twin query to execute against the end-point
   * @param schema The structure of the data being retrieved
   * @return A collection of rows with values retrieved from the twin
   */
  private[digitaltwin] def queryDigitalTwin(query: String, schema: StructType): Seq[Seq[Any]] = {
    val client = getHttpClient

    var continuationToken: Option[String] = None
    var collectedResults: Seq[Seq[Any]] = Seq()

    do {
      val (results, nextToken) = executeQuery(client, query, continuationToken)

      val parsedRows = results.values match {
        case rows: Seq[Map[String, AnyRef]] => rows.map(parseMapToSchema(_, schema))
        case _ => throw DigitalTwinClientException("Not yet implemented")
      }

      continuationToken = nextToken
      collectedResults ++= parsedRows
    } while (continuationToken.nonEmpty)

    collectedResults
  }

  /**
   * Attempts to parse a string as a [[java.sql.Timestamp]] value
   *
   * @param value the string value to parse
   * @return An [[Option]] containing the parsed [[java.sql.Timestamp]], or [[None]]
   */
  private def tryParseTimestamp(value: String): Option[Timestamp] = {
    val parsed = timestampFormats.flatMap(f => {
      val lt = Try(LocalDateTime.parse(value, f))
      val zt = Try(ZonedDateTime.parse(value, f))

      if (zt.isSuccess) Some(zt.get.toInstant)
      else if (lt.isSuccess) Some(lt.get.toInstant(ZoneOffset.UTC))
      else None
    })

    if (parsed.nonEmpty) Some(Timestamp.from(parsed.head)) else None
  }

  /**
   * Parses a [[Map]] into a sequence of values which correspond to a given [[StructType]]
   *
   * @param row    the map of values to parse
   * @param schema defines the layout of the map items
   * @return A sequence of values from the map which correspond to the schema
   */
  private def parseMapToSchema(row: Map[String, AnyRef], schema: StructType): Seq[Any] = {
    schema.fields.map(f => {
      val sourceName = f.name.split("__").head
      val col = row.get(sourceName)
      col match {
        case Some(c) => f.dataType match {
          case nested: StructType if c.isInstanceOf[Map[String, AnyRef]] =>
            parseMapToSchema(c.asInstanceOf[Map[String, AnyRef]], nested)
          case _: DoubleType if c.isInstanceOf[java.lang.Double] || c.isInstanceOf[scala.math.BigInt] => c match {
            case d: java.lang.Double => d
            case bi: scala.math.BigInt => bi.toDouble
          }
          case _: BooleanType if c.isInstanceOf[java.lang.Boolean] => c.asInstanceOf[java.lang.Boolean]
          case _: TimestampType if c.isInstanceOf[String] => tryParseTimestamp(c.asInstanceOf[String]).orNull
          case _: StringType if c.isInstanceOf[String] => c.asInstanceOf[String]
          case _ => null
        }
        case None => null
      }
    })
  }

  /**
   * Parses a Key-Value pair into a [[StructField]]
   *
   * @param key   the key of the pair
   * @param value the value of the pair
   * @return a [[StructField]] valid for the provided pair
   */
  private def parseToStructType(key: String, value: AnyRef): StructField = value match {
    case _: java.lang.Double => StructField(key, DoubleType, nullable = true)
    case _: scala.math.BigInt | _: java.lang.Long => StructField(key, DoubleType, nullable = true)
    case _: java.lang.Boolean => StructField(key, BooleanType, nullable = true)
    case ts: java.lang.String if tryParseTimestamp(ts).nonEmpty => StructField(key, TimestampType, nullable = true)
    case nested: Map[String, AnyRef] =>
      val nestedTypes = nested.map { case (nk, nv) => parseToStructType(nk, nv) }.toSeq
      StructField(key, StructType(nestedTypes), nullable = true)
    case _: java.lang.String => StructField(key, StringType, nullable = true)
    case _ => throw DigitalTwinClientException("Not yet implemented")
  }

  /**
   * Merge a given schema into an existing schema
   *
   * @param fields        new schema fields
   * @param currentSchema fields in the existing schema
   * @return a merged array of fields
   */
  private def mergeSchema(fields: Seq[StructField], currentSchema: Seq[StructField]): Seq[StructField] = {
    val newFields = fields.flatMap(f => {
      currentSchema.find(cs => cs.name == f.name) match {
        // If this is a struct type in both schemas, then merge them together and replace the current schema
        // item with the new schema
        case Some(cs) if cs.dataType.isInstanceOf[StructType] && f.dataType.isInstanceOf[StructType] =>
          val mergedNestedSchema = mergeSchema(f.dataType.asInstanceOf[StructType].fields, cs.dataType.asInstanceOf[StructType].fields)
          Some(StructField(f.name, StructType(mergedNestedSchema), nullable = true))

        // If the names and data types match then ignore this field as it's already handled
        case Some(cs) if cs.dataType == f.dataType => None

        // If the names match but data types differ then add a new field to handle the new type
        case Some(_) =>
          // Create a new field suffixed with an index (e.g. temp => temp__1)
          val newIndex = currentSchema.filter(cs => cs.name.matches(s"${f.name}__\\d+"))
            .map(_.name.stripPrefix(s"${f.name}__").toInt)
            .foldLeft(0)((acc, x) => Math.max(acc, x)) + 1
          Some(f.copy(name = s"${f.name}__$newIndex"))

        // Otherwise this is a completely new field so add it
        case _ => Some(f)
      }
    })

    // Remove the StructType fields from the current schema (as these are fully replaced)
    val newFieldNestedNames = newFields.filter(nf => nf.dataType.isInstanceOf[StructType]).map(_.name)
    val clearedSchema = currentSchema.filter(cs => !newFieldNestedNames.contains(cs.name))

    // Combine the fields to create a merged schema
    clearedSchema ++ newFields
  }

  /**
   * Infers the schema for a given query based on a subset of the data
   *
   * @param query the query to execute against the Digital Twin
   * @return a [[StructType]] which represents the data retrieved from the Digital Twin
   */
  private[digitaltwin] def inferQuerySchema(query: String): StructType = {
    val client = getHttpClient

    val limitedQuery = query match {
      case queryColumnRegex(c, q) => if (c == "*") {
        s"SELECT TOP(${options.limit}) FROM $q"
      } else {
        s"SELECT TOP(${options.limit}) $c FROM $q"
      }
      case _ => throw DigitalTwinClientException("Unable to parse query")
    }

    val (results, _) = executeQuery(client, limitedQuery, None)

    val parsedSchema = results.values match {
      case rows: Seq[Map[String, AnyRef]] => rows.map(r => r.map { case (k, v) => parseToStructType(k, v) }.toSeq)
      case _ => throw DigitalTwinClientException("Not yet implemented")
    }

    if (parsedSchema.isEmpty) {
      throw DigitalTwinClientException("Unable to infer a schema from an empty result set")
    }

    val inferredSchema = parsedSchema.tail.foldLeft(parsedSchema.head)((acc, s) => mergeSchema(s, acc))

    StructType(inferredSchema)
  }
}
