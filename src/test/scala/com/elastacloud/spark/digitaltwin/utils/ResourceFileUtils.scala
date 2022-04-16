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
