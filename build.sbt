import sbt.url

lazy val root = (project in file("."))
  .settings(commonSettings)

// Project settings
val sparkVersion = settingKey[String]("Spark version")

name := "spark-azure-digital-twin"
version := "0.1.0-SNAPSHOT"
organization := "com.elastacloud"
description := "Spark data source for Azure Digital Twin"
homepage := Some(url("https://www.elastacloud.com"))
developers ++= List(
  Developer(id = "dazfuller", name = "Darren Fuller", email = "darren@elastacloud.com", url = url("https://github.com/dazfuller"))
)
target := file("target") / s"spark-${sparkVersion.value}"

ThisBuild / artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  s"${artifact.name}_${sv.binary}-${sparkVersion.value}_${module.revision}.${artifact.extension}"
}

// Main project dependencies
ThisBuild / libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion.value % Provided,
  "commons-validator" % "commons-validator" % "1.7",
  "org.apache.httpcomponents" % "httpclient" % "4.5.13"
)

// Shading and assembly configuration
ThisBuild / assemblyShadeRules := Seq(
  ShadeRule.rename("org.apache.commons.validator.**" -> "elastashade.validator.@1").inAll,
  ShadeRule.rename("org.apache.http.**" -> "elastashade.http.@1").inAll
)

ThisBuild / assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _@_*) => MergeStrategy.first
  case PathList("com", "elastacloud", _@_*) => MergeStrategy.last
  case PathList("elastashade", "validator", _@_*) => MergeStrategy.last
  case PathList("elastashade", "http", _@_*) => MergeStrategy.last
  case _ => MergeStrategy.discard
}

assembly / assemblyOption ~= {
  _.withIncludeScala(false)
}

assembly / assemblyJarName := s"${name.value}_uber_${scalaBinaryVersion.value}-${sparkVersion.value}_${version.value}.jar"

// Testing dependencies
ThisBuild / libraryDependencies ++= Seq(
  "io.netty" % "netty-transport-native-epoll" % "4.1.68.Final" % Test, // Added to work around issue in IntelliJ but should not be required for deployment
  "org.scalactic" %% "scalactic" % "3.2.12",
  "org.scalatest" %% "scalatest" % "3.2.12" % Test,
  "org.mockito" %% "mockito-scala" % "1.17.5" % Test
)

// Code coverage configuration
coverageOutputCobertura := true
coverageOutputHTML := true
coverageMinimumStmtTotal := 70
coverageFailOnMinimum := false
coverageHighlighting := true

val commonSettings = Seq(
  sparkVersion := System.getProperty("sparkVersion", "3.2.1"),
  scalaVersion := {
    if (sparkVersion.value >= "3.2.0") {
      "2.12.14"
    } else {
      "2.12.10"
    }
  }
)
