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

package com.elastacloud.spark.digitaltwin.utils

import java.net.URI
import scala.io.{BufferedSource, Source}

object ResourceFileUtils {
  /**
   * Get the path to a resource file
   *
   * @param relativePath the relative path of the resource file to the resources directory
   * @return the path of the resource file
   */
  def getResourceFilePath(relativePath: String): String = {
    getClass.getResource(relativePath).getPath
  }

  /**
   * Get the string content of a resource file
   *
   * @param relativePath the relative path of the resource file to the resources directory
   * @return the content of the file as a string
   */
  def getFileContent(relativePath: String): String = {
    val scriptSourcePath = getResourceFilePath(relativePath)
    var scriptSource: BufferedSource = null

    try {
      scriptSource = Source.fromFile(scriptSourcePath)
      scriptSource.getLines().mkString("\n")
    } finally {
      if (scriptSource != null) scriptSource.close()
    }
  }

  /**
   * Get the root path of the resources location
   *
   * @return the path to the resources as a file URI
   */
  def getResourceRoot: URI = {
    getClass.getResource("/").toURI
  }
}
