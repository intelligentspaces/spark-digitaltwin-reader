package com.elastacloud.spark.digitaltwin

import com.elastacloud.spark.digitaltwin.utils.ResourceFileUtils
import org.apache.http.StatusLine
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost, HttpUriRequest}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.types._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.mockito.ArgumentMatchers.any
import org.mockito.{ArgumentCaptor, MockitoSugar}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.{OffsetDateTime, ZoneOffset}

class DigitalTwinClientTests extends AnyFlatSpec with Matchers with MockitoSugar {
  private val dummyOptions = DigitalTwinClientOptions.from(Map(
    "tenantId" -> "abc-tenant-123",
    "clientId" -> "abc-client-123",
    "clientSecret" -> "super-secret-password",
    "endpoint" -> "https://myinstance.api.weu.digitaltwins.azure.net",
    "query" -> "SELECT * FROM digitaltwins"
  ))

  implicit val formats: DefaultFormats.type = DefaultFormats

  /**
   * Compares two schemas by comparing fields between them and working through nested structures
   *
   * @param actual   the schema which has been derived
   * @param expected the schema which is expected
   */
  private def validateStructTypes(actual: StructType, expected: StructType): Unit = {
    val expectedFields = expected.fields.filter(!_.dataType.isInstanceOf[StructType])
    val actualFields = actual.fields.filter(!_.dataType.isInstanceOf[StructType])

    actualFields should contain theSameElementsAs expectedFields

    val expectedNested = expected.fields.filter(_.dataType.isInstanceOf[StructType])
    val actualNested = actual.fields.filter(_.dataType.isInstanceOf[StructType])

    actualNested.length should be(expectedNested.length)

    actualNested.foreach(a => {
      val matchingExpectedField = expectedNested.find(e => e.name == a.name)
      matchingExpectedField match {
        case Some(value) => validateStructTypes(a.dataType.asInstanceOf[StructType], value.dataType.asInstanceOf[StructType])
        case None => fail(s"StructType field '${a.name}' was not expected")
      }
    })
  }

  private def validateInferringSchema(sourceDataPath: String, expectedSchema: StructType): Unit = {
    val httpClient = mock[CloseableHttpClient]

    val authResponse = mock[CloseableHttpResponse]
    val authStatusLine = mock[StatusLine]
    val authResponseEntity = new StringEntity(ResourceFileUtils.getFileContent("/client-tests/mock-auth-response.json"))

    when(authResponse.getStatusLine).thenReturn(authStatusLine)
    when(authResponse.getEntity).thenReturn(authResponseEntity)
    when(authStatusLine.getStatusCode).thenReturn(value = 200)

    val queryResponse = mock[CloseableHttpResponse]
    val queryStatusLine = mock[StatusLine]
    val queryResponseEntity = new StringEntity(ResourceFileUtils.getFileContent(sourceDataPath))

    when(queryResponse.getStatusLine).thenReturn(queryStatusLine)
    when(queryResponse.getEntity).thenReturn(queryResponseEntity)
    when(queryStatusLine.getStatusCode).thenReturn(value = 200)

    when(httpClient.execute(any[HttpUriRequest]))
      .thenReturn(authResponse)
      .andThen(queryResponse)

    val client = new TestDigitalTwinClient(httpClient, dummyOptions)
    val schema = client.inferQuerySchema("SELECT * FROM digitaltwins")

    val httpRequestArgs = ArgumentCaptor.forClass(classOf[HttpUriRequest])
    verify(httpClient, times(2)).execute(httpRequestArgs.capture())
    verify(authResponse, times(1)).getEntity
    verify(queryResponse, times(1)).getEntity

    val queryPostRequest = httpRequestArgs.getAllValues.get(1).asInstanceOf[HttpPost]
    val postQuery = (parse(EntityUtils.toString(queryPostRequest.getEntity)) \ "query").extract[Option[String]]

    postQuery shouldNot be(empty)
    postQuery.get should be(s"SELECT TOP(${dummyOptions.limit}) FROM digitaltwins")

    validateStructTypes(schema, expectedSchema)
  }

  "Authenticating with Azure" should "successfully extract a bearer token" in {
    val httpClient = mock[CloseableHttpClient]

    val authResponse = mock[CloseableHttpResponse]
    val authStatusLine = mock[StatusLine]
    val authResponseEntity = new StringEntity(ResourceFileUtils.getFileContent("/client-tests/mock-auth-response.json"))

    when(authResponse.getStatusLine).thenReturn(authStatusLine)
    when(authResponse.getEntity).thenReturn(authResponseEntity)
    when(authStatusLine.getStatusCode).thenReturn(value = 200)

    when(httpClient.execute(any[HttpUriRequest]))
      .thenReturn(authResponse)

    val client = new TestDigitalTwinClient(httpClient, dummyOptions)
    val bearerToken = client.getBearerToken

    bearerToken should be("some_very_very_long_token_value")
  }

  it should "use the current authentication token if it is still valid" in {
    val httpClient = mock[CloseableHttpClient]

    val authResponse = mock[CloseableHttpResponse]
    val authStatusLine = mock[StatusLine]
    val expiryTime = OffsetDateTime.now(ZoneOffset.UTC).plusMinutes(1).toEpochSecond
    val authResponseEntity = new StringEntity(
      s"""{
         |  "token_type": "Bearer",
         |  "expires_in": "3599",
         |  "ext_expires_in": "3599",
         |  "expires_on": "$expiryTime",
         |  "not_before": "$expiryTime",
         |  "resource": "0b07f429-9f4b-4714-9392-cc5e8e80c8b0",
         |  "access_token": "a_bearer_token"
         |}""".stripMargin)

    when(authResponse.getStatusLine).thenReturn(authStatusLine)
    when(authResponse.getEntity).thenReturn(authResponseEntity)
    when(authStatusLine.getStatusCode).thenReturn(value = 200)

    when(httpClient.execute(any[HttpUriRequest]))
      .thenReturn(authResponse)

    val client = new TestDigitalTwinClient(httpClient, dummyOptions)
    client.getBearerToken

    val bearerToken = client.getBearerToken

    bearerToken should be("a_bearer_token")
    verify(httpClient, times(1)).execute(any[HttpUriRequest])
  }

  it should "refresh the token if it has expired" in {
    val httpClient = mock[CloseableHttpClient]

    val authResponse = mock[CloseableHttpResponse]
    val authStatusLine = mock[StatusLine]
    // The mock response has a timestamp in the past
    val authResponseEntity = new StringEntity(ResourceFileUtils.getFileContent("/client-tests/mock-auth-response.json"))

    when(authResponse.getStatusLine).thenReturn(authStatusLine)
    when(authResponse.getEntity).thenReturn(authResponseEntity)
    when(authStatusLine.getStatusCode).thenReturn(value = 200)

    when(httpClient.execute(any[HttpUriRequest]))
      .thenReturn(authResponse)

    val client = new TestDigitalTwinClient(httpClient, dummyOptions)
    client.getBearerToken

    val bearerToken = client.getBearerToken

    bearerToken should be("some_very_very_long_token_value")
    verify(httpClient, times(2)).execute(any[HttpUriRequest])
  }

  it should "throw an error if the client receives a non-success error code" in {
    val httpClient = mock[CloseableHttpClient]

    val authResponse = mock[CloseableHttpResponse]
    val authStatusLine = mock[StatusLine]

    when(authResponse.getStatusLine).thenReturn(authStatusLine)
    when(authStatusLine.getStatusCode).thenReturn(value = 400)

    when(httpClient.execute(any[HttpUriRequest]))
      .thenReturn(authResponse)

    val client = new TestDigitalTwinClient(httpClient, dummyOptions)
    val exception = the[DigitalTwinClientException] thrownBy client.getBearerToken

    verify(httpClient, times(1)).execute(any[HttpUriRequest])
    exception.message.contains("Unable to acquire client token: 400") should be(true)
  }

  "Inferring the schema from a query" should "provide a valid schema for tabular type data" in {
    val expectedSchema = new StructType(Array(
      StructField("$dtId", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("logo", StringType, nullable = true),
      StructField("primaryCssFile", StringType, nullable = true),
      StructField("nameLastUpdated", TimestampType, nullable = true)
    ))

    validateInferringSchema("/client-tests/tabulardata.json", expectedSchema)
  }

  it should "provide a valid schema for mixed type data" in {
    val expectedSchema = new StructType(Array(
      StructField("$dtId", StringType, nullable = true),
      StructField("dt", StructType(Array(
        StructField("$dtId", StringType, nullable = true),
        StructField("$etag", StringType, nullable = true),
        StructField("logo", StringType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("primaryCssFile", StringType, nullable = true),
        StructField("$metadata", StructType(Array(
          StructField("$model", StringType, nullable = true),
          StructField("logo", StructType(Array(
            StructField("lastUpdateTime", TimestampType, nullable = true),
          )), nullable = true),
          StructField("name", StructType(Array(
            StructField("lastUpdateTime", TimestampType, nullable = true),
          )), nullable = true),
          StructField("primaryCssFile", StructType(Array(
            StructField("lastUpdateTime", TimestampType, nullable = true),
          )), nullable = true),
        )), nullable = true),
      )), nullable = true)
    ))

    validateInferringSchema("/client-tests/mixed-format-data.json", expectedSchema)
  }

  it should "generate a combined schema if the results vary in structure" in {
    val expectedSchema = new StructType(Array(
      StructField("$dtId", StringType, nullable = true),
      StructField("dt", StructType(Array(
        StructField("$dtId", StringType, nullable = true),
        StructField("data", StructType(Array(
          StructField("temperature", DoubleType, nullable = true),
          StructField("co2", DoubleType, nullable = true),
          StructField("pressure", StringType, nullable = true),
          StructField("pressure__1", DoubleType, nullable = true)
        )), nullable = true),
      )), nullable = true)
    ))

    validateInferringSchema("/client-tests/variable-schema-data.json", expectedSchema)
  }

  it should "throw an error if no results are returned from the query" in {
    val exception = the[DigitalTwinClientException] thrownBy validateInferringSchema("/client-tests/empty-results.json", new StructType())
    exception.message should startWith("Unable to infer a schema from an empty result set")
  }

  "Querying the digital twin" should "load structured data for semi-structured data" in {
    val httpClient = mock[CloseableHttpClient]

    val authResponse = mock[CloseableHttpResponse]
    val authStatusLine = mock[StatusLine]
    val authResponseEntity = new StringEntity(ResourceFileUtils.getFileContent("/client-tests/mock-auth-response.json"))

    when(authResponse.getStatusLine).thenReturn(authStatusLine)
    when(authResponse.getEntity).thenReturn(authResponseEntity)
    when(authStatusLine.getStatusCode).thenReturn(value = 200)

    val queryResponse = mock[CloseableHttpResponse]
    val queryStatusLine = mock[StatusLine]
    val queryResponseEntity = new StringEntity(ResourceFileUtils.getFileContent("/client-tests/mixed-format-data.json"))

    when(queryResponse.getStatusLine).thenReturn(queryStatusLine)
    when(queryResponse.getEntity).thenReturn(queryResponseEntity)
    when(queryStatusLine.getStatusCode).thenReturn(value = 200)

    when(httpClient.execute(any[HttpUriRequest]))
      .thenReturn(authResponse)
      .andThen(queryResponse)
      .andThen(authResponse) // Needed as the mock response has an expiry date in the past
      .andThen(queryResponse)

    val client = new TestDigitalTwinClient(httpClient, dummyOptions)
    val schema = client.inferQuerySchema("SELECT * FROM digitaltwins")
    val results = client.queryDigitalTwin("SELECT * FROM digitaltwins", schema)

    results shouldNot be(empty)
    results.length should be(3)
  }

  it should "load data which arrives in different structures for each row" in {
    val httpClient = mock[CloseableHttpClient]

    val authResponse = mock[CloseableHttpResponse]
    val authStatusLine = mock[StatusLine]
    val authResponseEntity = new StringEntity(ResourceFileUtils.getFileContent("/client-tests/mock-auth-response.json"))

    when(authResponse.getStatusLine).thenReturn(authStatusLine)
    when(authResponse.getEntity).thenReturn(authResponseEntity)
    when(authStatusLine.getStatusCode).thenReturn(value = 200)

    val queryResponse = mock[CloseableHttpResponse]
    val queryStatusLine = mock[StatusLine]
    val queryResponseEntity = new StringEntity(ResourceFileUtils.getFileContent("/client-tests/variable-schema-data.json"))

    when(queryResponse.getStatusLine).thenReturn(queryStatusLine)
    when(queryResponse.getEntity).thenReturn(queryResponseEntity)
    when(queryStatusLine.getStatusCode).thenReturn(value = 200)

    when(httpClient.execute(any[HttpUriRequest]))
      .thenReturn(authResponse)
      .andThen(queryResponse)
      .andThen(authResponse) // Needed as the mock response has an expiry date in the past
      .andThen(queryResponse)

    val expectedComp2 = Seq(
      "Comp2",
      Seq(
        "Comp2",
        Seq(
          null,
          500.556,
          "GOOD",
          null
        )
      )
    )

    val expectedComp3 = Seq(
      "Comp3",
      Seq(
        "Comp3",
        Seq(
          20.13,
          800.1,
          null,
          1019.0
        )
      )
    )

    val client = new TestDigitalTwinClient(httpClient, dummyOptions)
    val schema = client.inferQuerySchema("SELECT * FROM digitaltwins")
    val results = client.queryDigitalTwin("SELECT * FROM digitaltwins", schema)

    results shouldNot be(empty)
    results(1) should contain theSameElementsAs expectedComp2
    results(2) should contain theSameElementsAs expectedComp3
  }

  it should "handle multiple data types in the query response" in {
    val httpClient = mock[CloseableHttpClient]

    val authResponse = mock[CloseableHttpResponse]
    val authStatusLine = mock[StatusLine]
    val authResponseEntity = new StringEntity(ResourceFileUtils.getFileContent("/client-tests/mock-auth-response.json"))

    when(authResponse.getStatusLine).thenReturn(authStatusLine)
    when(authResponse.getEntity).thenReturn(authResponseEntity)
    when(authStatusLine.getStatusCode).thenReturn(value = 200)

    val queryResponse = mock[CloseableHttpResponse]
    val queryStatusLine = mock[StatusLine]
    val queryResponseEntity = new StringEntity(ResourceFileUtils.getFileContent("/client-tests/tabular-types-data.json"))

    when(queryResponse.getStatusLine).thenReturn(queryStatusLine)
    when(queryResponse.getEntity).thenReturn(queryResponseEntity)
    when(queryStatusLine.getStatusCode).thenReturn(value = 200)

    when(httpClient.execute(any[HttpUriRequest]))
      .thenReturn(authResponse)
      .andThen(queryResponse)
      .andThen(authResponse) // Needed as the mock response has an expiry date in the past
      .andThen(queryResponse)

    val expectedResults = Seq(
      "A simple string",
      1234567890,
      123.456,
      Timestamp.valueOf("2022-03-20 17:12:45.000000"),
      true,
      Seq(
        false
      )
    )

    val client = new TestDigitalTwinClient(httpClient, dummyOptions)
    val schema = client.inferQuerySchema("SELECT * FROM digitaltwins")
    val results = client.queryDigitalTwin("SELECT * FROM digitaltwins", schema)

    results shouldNot be(empty)
    results.length should be(1)

    results.head should contain theSameElementsAs expectedResults
  }

  it should "page results when a continuation token is available" in {
    val httpClient = mock[CloseableHttpClient]

    val authResponse = mock[CloseableHttpResponse]
    val authStatusLine = mock[StatusLine]
    val expiryTime = OffsetDateTime.now(ZoneOffset.UTC).plusMinutes(1).toEpochSecond
    val authResponseEntity = new StringEntity(
      s"""{
         |  "token_type": "Bearer",
         |  "expires_in": "3599",
         |  "ext_expires_in": "3599",
         |  "expires_on": "$expiryTime",
         |  "not_before": "$expiryTime",
         |  "resource": "0b07f429-9f4b-4714-9392-cc5e8e80c8b0",
         |  "access_token": "a_bearer_token"
         |}""".stripMargin)

    when(authResponse.getStatusLine).thenReturn(authStatusLine)
    when(authResponse.getEntity).thenReturn(authResponseEntity)
    when(authStatusLine.getStatusCode).thenReturn(value = 200)

    val queryResponse = mock[CloseableHttpResponse]
    val queryStatusLine = mock[StatusLine]
    val queryResponseEntitySchema = new StringEntity("""{ "value": [ { "col1": "value 1" } ], "continuationToken": null }""")
    val queryResponseEntity1 = new StringEntity("""{ "value": [ { "col1": "value 1" }, { "col1": "value 2" } ], "continuationToken": "{\"_t\":1,\"_s\":100,\"_rc\":null,\"id\":\"abc123\"}" }""")
    val queryResponseEntity2 = new StringEntity("""{ "value": [ { "col1": "value 3" }, { "col1": "value 4" } ], "continuationToken": "{\"_t\":1,\"_s\":100,\"_rc\":null,\"id\":\"abc234\"}" }""")
    val queryResponseEntity3 = new StringEntity("""{ "value": [ { "col1": "value 5" }, { "col1": "value 6" } ], "continuationToken": null }""")

    when(queryResponse.getStatusLine).thenReturn(queryStatusLine)
    when(queryResponse.getEntity)
      .thenReturn(queryResponseEntitySchema)
      .andThen(queryResponseEntity1)
      .andThen(queryResponseEntity2)
      .andThen(queryResponseEntity3)
    when(queryStatusLine.getStatusCode).thenReturn(value = 200)

    when(httpClient.execute(any[HttpUriRequest]))
      .thenReturn(authResponse)
      .andThen(queryResponse)

    val expectedResults = Seq(
      Seq("value 1"),
      Seq("value 2"),
      Seq("value 3"),
      Seq("value 4"),
      Seq("value 5"),
      Seq("value 6")
    )

    val client = new TestDigitalTwinClient(httpClient, dummyOptions)
    val schema = client.inferQuerySchema("SELECT dt, b FROM digitaltwins dt JOIN buildings b")
    val results = client.queryDigitalTwin("SELECT dt, b FROM digitaltwins dt JOIN buildings b", schema)

    val httpRequestArgs = ArgumentCaptor.forClass(classOf[HttpUriRequest])
    verify(httpClient, times(5)).execute(httpRequestArgs.capture())

    val pagedQueryRequest = httpRequestArgs.getAllValues.get(3).asInstanceOf[HttpPost]
    val pagedQuery = (parse(EntityUtils.toString(pagedQueryRequest.getEntity)) \ "continuationToken").extract[Option[String]]

    results shouldNot be(empty)
    results should contain theSameElementsAs expectedResults
    pagedQuery shouldBe Some("{\"_t\":1,\"_s\":100,\"_rc\":null,\"id\":\"abc123\"}")
  }

  it should "throw an error if the API returns a non-successful response code" in {
    val httpClient = mock[CloseableHttpClient]

    val authResponse = mock[CloseableHttpResponse]
    val authStatusLine = mock[StatusLine]
    val authResponseEntity = new StringEntity(ResourceFileUtils.getFileContent("/client-tests/mock-auth-response.json"))

    when(authResponse.getStatusLine).thenReturn(authStatusLine)
    when(authResponse.getEntity).thenReturn(authResponseEntity)
    when(authStatusLine.getStatusCode).thenReturn(value = 200)

    val queryResponse = mock[CloseableHttpResponse]
    val queryStatusLine = mock[StatusLine]
    val queryResponseEntity = new StringEntity(ResourceFileUtils.getFileContent("/client-tests/tabular-types-data.json"))

    when(queryResponse.getStatusLine).thenReturn(queryStatusLine)
    when(queryResponse.getEntity).thenReturn(queryResponseEntity)
    when(queryStatusLine.getStatusCode).thenReturn(value = 200)

    val errorResponse = mock[CloseableHttpResponse]
    val errorStatusLine = mock[StatusLine]
    val errorResponseEntity = new StringEntity("Request failed")

    when(errorResponse.getStatusLine).thenReturn(errorStatusLine)
    when(errorResponse.getEntity).thenReturn(errorResponseEntity)
    when(errorStatusLine.getStatusCode).thenReturn(value = 400)

    when(httpClient.execute(any[HttpUriRequest]))
      .thenReturn(authResponse)
      .andThen(queryResponse)
      .andThen(authResponse) // Needed as the mock response has an expiry date in the past
      .andThen(errorResponse)

    val client = new TestDigitalTwinClient(httpClient, dummyOptions)
    val schema = client.inferQuerySchema("SELECT dt, b FROM digitaltwins dt JOIN buildings b")
    val exception = the[DigitalTwinClientException] thrownBy client.queryDigitalTwin("SELECT dt, b FROM digitaltwins dt JOIN buildings b", schema)

    exception.message should startWith("Digital twin returned an unsuccessful status code: 400")
  }
}
