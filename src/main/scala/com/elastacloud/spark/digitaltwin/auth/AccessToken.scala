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

package com.elastacloud.spark.digitaltwin.auth

case class AccessToken(token_type: String, expires_in: String, ext_expires_in: String, expires_on: String, not_before: String, resource: String, access_token: String) {
  def expires_on_unix: Long = expires_on.toLong

  def not_before_unix: Long = not_before.toLong
}
