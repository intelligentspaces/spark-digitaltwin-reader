package com.elastacloud.spark.digitaltwin.auth

case class AccessToken(token_type: String, expires_in: String, ext_expires_in: String, expires_on: String, not_before: String, resource: String, access_token: String) {
  def expires_on_unix: Long = expires_on.toLong

  def not_before_unix: Long = not_before.toLong
}
