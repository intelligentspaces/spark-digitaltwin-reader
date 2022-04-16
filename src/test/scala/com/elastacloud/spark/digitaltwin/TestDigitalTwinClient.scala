package com.elastacloud.spark.digitaltwin

import org.apache.http.impl.client.CloseableHttpClient

class TestDigitalTwinClient(httpClient: CloseableHttpClient, options: DigitalTwinClientOptions) extends DigitalTwinClient(options) {
  override protected def getHttpClient: CloseableHttpClient = httpClient

  override def getBearerToken: String = super.getBearerToken
}
