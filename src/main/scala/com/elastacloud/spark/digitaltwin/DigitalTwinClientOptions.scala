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

import org.apache.commons.validator.routines.UrlValidator
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ArrayBuffer

/**
 * Defines the options available when querying a Digital Twin
 *
 * @param tenantId     the tenant id of the service principal being used for connectivity
 * @param clientId     the client id of the service principal being used for connectivity
 * @param clientSecret the client secret of the service principal being used for connectivity
 * @param endpoint     endpoint of the Digital Twin
 * @param limit        number of records to read when inferring a schema
 * @param query        the query to execute against the digital twin
 */
private[digitaltwin] case class DigitalTwinClientOptions(tenantId: String,
                                                         clientId: String,
                                                         clientSecret: String,
                                                         endpoint: String,
                                                         limit: Int,
                                                         query: String)

private[digitaltwin] object DigitalTwinClientOptions {
  val defaultLimit = 30

  /**
   * Validates that the required keys have been provided by the user and are valid
   *
   * @param keys collection of [[String]] values to check
   */
  def validateRequiredOptions(keys: Set[String]): Unit = {
    val errorBuffer = new ArrayBuffer[String]()

    if (!keys.exists(_.compareToIgnoreCase("tenantId") == 0)) {
      errorBuffer.append("tenantId")
    }

    if (!keys.exists(_.compareToIgnoreCase("clientId") == 0)) {
      errorBuffer.append("clientId")
    }

    if (!keys.exists(_.compareToIgnoreCase("clientSecret") == 0)) {
      errorBuffer.append("clientSecret")
    }

    if (!keys.exists(_.compareToIgnoreCase("endpoint") == 0)) {
      errorBuffer.append("endpoint")
    }

    if (!keys.exists(_.compareToIgnoreCase("query") == 0)) {
      errorBuffer.append("query")
    }

    if (errorBuffer.nonEmpty) {
      throw DigitalTwinClientOptionsException(s"The following options must be specified: ${errorBuffer.mkString(", ")}")
    }
  }

  def validateEndpoint(endpoint: String): Unit = {
    val validSchemes = Array("https")
    val validator = new UrlValidator(validSchemes)

    if (!validator.isValid(endpoint)) {
      throw DigitalTwinClientOptionsException(s"The provided endpoint '$endpoint' is not valid. Valid URLs over HTTPS should be provided.")
    }
  }

  def validateQuery(query: String): String = {
    val cleanedQuery = query.trim
      .replaceAll("""\s+""", " ")
      .replaceAll("""(\r\n)|\r|\n""", " ")
      .trim

    val casedQuery = cleanedQuery.toLowerCase

    if (casedQuery.isEmpty) {
      throw DigitalTwinClientOptionsException("A non-empty query must be specified.")
    } else if (casedQuery.substring(0, 6).compareTo("select") != 0) {
      throw DigitalTwinClientOptionsException("A query must begin with SELECT.")
    } else if (!casedQuery.contains("from digitaltwins") && !casedQuery.contains("from relationships")) {
      throw DigitalTwinClientOptionsException("A query must contain a FROM DIGITALTWINS or FROM RELATIONSHIPS clause.")
    }

    cleanedQuery
  }

  private def validateLimit(limit: Int): Unit = {
    if (limit <= 0 || limit > 100) {
      throw DigitalTwinClientOptionsException("Limit for inferring schema must be between 1 and 100")
    }
  }

  /**
   * Parses options from a [[CaseInsensitiveStringMap]] collection
   *
   * @param options collection containing possible parameter values
   * @return a [[DigitalTwinClientOptions]] if valid
   * @throws DigitalTwinClientOptionsException if the options are not valid
   */
  def from(options: CaseInsensitiveStringMap): DigitalTwinClientOptions = {
    validateRequiredOptions(options.keySet().toSet)
    validateEndpoint(options.get("endpoint"))
    val query = validateQuery(options.get("query"))
    val limit = options.getInt("limit", defaultLimit)
    validateLimit(limit)

    DigitalTwinClientOptions(
      options.get("tenantId"),
      options.get("clientId"),
      options.get("clientSecret"),
      options.get("endpoint"),
      limit,
      query
    )
  }

  /**
   * Parses options from a [[Predef.Map]] collection
   *
   * @param options collection containing possible parameter values
   * @return a [[DigitalTwinClientOptions]] if valid
   * @throws DigitalTwinClientOptionsException if the options are not valid
   */
  def from(options: Map[String, String]): DigitalTwinClientOptions = {
    validateRequiredOptions(options.keys.toSet)
    validateEndpoint(options("endpoint"))
    val query = validateQuery(options("query"))
    val limit = options.getOrElse("limit", defaultLimit.toString).toInt
    validateLimit(limit)

    DigitalTwinClientOptions(
      options("tenantId"),
      options("clientId"),
      options("clientSecret"),
      options("endpoint"),
      limit,
      query
    )
  }
}
