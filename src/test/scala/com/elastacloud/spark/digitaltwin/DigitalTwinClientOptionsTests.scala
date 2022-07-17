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

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters.mapAsJavaMapConverter

class DigitalTwinClientOptionsTests extends AnyFlatSpec with Matchers {
  "Creating from a case-insensitive map" should "use all required values when creating options" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
      "limit" -> "10",
      "query" -> "SELECT * FROM DIGITALTWINS"
    ).asJava)

    val options = DigitalTwinClientOptions.from(input)

    options.tenantId should be("the-tenant-id")
    options.clientId should be("some-client-id")
    options.clientSecret should be("super-secret-value")
    options.endpoint should be("https://my-endpoint.digitaltwins.azure.net")
    options.limit should be(10)
    options.query should be("SELECT * FROM DIGITALTWINS")
  }

  it should "clean a query string ready for execution against a digital twin" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
      "limit" -> "10",
      "query" ->
        """
          |
          | select              *
          | from   digitaltwins
          | where IS_OF_MODEL('modelid')
          |""".stripMargin
    ).asJava)

    val options = DigitalTwinClientOptions.from(input)
    options.query should be("select * from digitaltwins where IS_OF_MODEL('modelid')")
  }

  it should "throw an error if the endpoint is not a valid url" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "invalid-endpoint",
      "query" -> "SELECT * FROM DIGITALTWINS"
    ).asJava)

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage.contains("endpoint 'invalid-endpoint' is not valid") should be(true)
  }

  it should "throw an error if the endpoint is not over https" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "http://my-endpoint.digitaltwins.azure.net",
      "query" -> "SELECT * FROM DIGITALTWINS"
    ).asJava)

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage.contains("endpoint 'http://my-endpoint.digitaltwins.azure.net' is not valid") should be(true)
  }

  it should "throw an error if the endpoint is not provided" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "query" -> "SELECT * FROM DIGITALTWINS"
    ).asJava)

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage.contains("The following options must be specified") should be(true)
    exception.getMessage.contains("endpoint") should be(true)
  }

  it should "throw an error if the tenantId is not provided" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
      "query" -> "SELECT * FROM DIGITALTWINS"
    ).asJava)

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage.contains("The following options must be specified") should be(true)
    exception.getMessage.contains("tenantId") should be(true)
  }

  it should "throw an error if the clientId is not provided" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
      "query" -> "SELECT * FROM DIGITALTWINS"
    ).asJava)

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage.contains("The following options must be specified") should be(true)
    exception.getMessage.contains("clientId") should be(true)
  }

  it should "throw an error if the clientSecret is not provided" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
      "query" -> "SELECT * FROM DIGITALTWINS"
    ).asJava)

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage.contains("The following options must be specified") should be(true)
    exception.getMessage.contains("clientSecret") should be(true)
  }

  it should "throw an error if the query is not provided" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net"
    ).asJava)

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage.contains("The following options must be specified") should be(true)
    exception.getMessage.contains("query") should be(true)
  }

  it should "throw an error if the query is empty" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
      "query" -> ""
    ).asJava)

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage.contains("A non-empty query must be specified") should be(true)
  }

  it should "throw an error if the query contains only white space" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
      "query" -> "      "
    ).asJava)

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage.contains("A non-empty query must be specified") should be(true)
  }

  it should "throw an error if the query does not contain a select statement" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
      "query" -> "FROM digitaltwins WHERE $dtId = 'some value'"
    ).asJava)

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage.contains("A query must begin with SELECT") should be(true)
  }

  it should "throw an error if the query does not contain a valid FROM statment" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
      "query" -> "SELECT * FROM rooms WHERE $dtId = 'some value'"
    ).asJava)

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage.contains("A query must contain a FROM DIGITALTWINS or FROM RELATIONSHIPS clause") should be(true)
  }

  it should "throw an error if the limit specified is below the valid range" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
      "limit" -> "-1",
      "query" -> "SELECT * FROM DIGITALTWINS"
    ).asJava)

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage should be("Limit for inferring schema must be between 1 and 100")
  }

  it should "throw an error if the limit specified is above the valid range" in {
    val input = new CaseInsensitiveStringMap(Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
      "limit" -> "1000",
      "query" -> "SELECT * FROM DIGITALTWINS"
    ).asJava)

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage should be("Limit for inferring schema must be between 1 and 100")
  }

  "Creating from a string map" should "use all required values when creating options" in {
    val input = Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
      "query" -> "SELECT * FROM DIGITALTWINS"
    )

    val options = DigitalTwinClientOptions.from(input)

    options.tenantId should be("the-tenant-id")
    options.clientId should be("some-client-id")
    options.clientSecret should be("super-secret-value")
    options.limit should be(30)
    options.endpoint should be("https://my-endpoint.digitaltwins.azure.net")
  }

  it should "throw an error if the endpoint is not a valid url" in {
    val input = Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "invalid-endpoint",
      "query" -> "SELECT * FROM DIGITALTWINS"
    )

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage.contains("endpoint 'invalid-endpoint' is not valid") should be(true)
  }

  it should "throw an error if the limit is not valid" in {
    val input = Map[String, String](
      "tenantId" -> "the-tenant-id",
      "clientId" -> "some-client-id",
      "clientSecret" -> "super-secret-value",
      "endpoint" -> "https://my-endpoint.digitaltwins.azure.net",
      "limit" -> "1000",
      "query" -> "SELECT * FROM DIGITALTWINS"
    )

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage should be("Limit for inferring schema must be between 1 and 100")
  }

  it should "throw an error if the required options are not provided" in {
    val input = Map[String, String]()

    val exception = the[DigitalTwinClientOptionsException] thrownBy DigitalTwinClientOptions.from(input)

    exception.getMessage.contains("The following options must be specified") should be(true)
    exception.getMessage.contains("tenantId, clientId, clientSecret, endpoint, query") should be(true)
  }
}
