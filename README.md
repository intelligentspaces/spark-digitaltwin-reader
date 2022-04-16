# Azure Digital Twin - Apache Speak Reader

[![spark-digitaltwin-reader-ci](https://github.com/intelligentspaces/spark-digitaltwin-reader/actions/workflows/spark-reader-ci.yml/badge.svg)](https://github.com/intelligentspaces/spark-digitaltwin-reader/actions/workflows/spark-reader-ci.yml)

An [Apache Spark](https://spark.apache.org) data source for [Azure Digital Twin](https://azure.microsoft.com/services/digital-twins/).

Connecting your Apache Spark instance to your Azure Digital Twin, allowing you to query just as any other data source
so that you can pull data into your Big Data processing pipelines based on the most recent information in your twin.

## Usage

Download the jar file for your version of Apache Spark (v3 and above) and make sure that the jar file is in the classpath.
For Azure Synapse Analytics and Databricks you can refer to the following documentation.

* [Databricks](https://docs.databricks.com/libraries/index.html)
* [Azure Synapse Analytics](https://docs.microsoft.com/azure/synapse-analytics/spark/apache-spark-azure-portal-add-libraries)

Once in your workspace you can use library just like any other data source, using either the fully qualified name
or the short name.

```scala
val df = spark.read.format("com.elastacloud.spark.digitaltwin").options(options).load()

val df = spark.read.format("digitaltwin").options(options).load()
```

Or, in Scala, you can import the library and use the helper function

```scala
import com.elastacloud.spark.digitaltwin._

val df = spark.read.options(options).digitaltwin()
```

## Options

As with any data source the digital twin reader relies on options to define how the instance will be accessed. The following
are the available options.

| Option       | Type   | Default | Description                                                                     |
|--------------|--------|---------|---------------------------------------------------------------------------------|
| tenantId     | string | N/A     | The tenant against which to authenticate the supplied client credentials        |
| clientID     | string | N/A     | The id of the service principal account used for connecting to the digital twin |
| clientSecret | string | N/A     | The secret of the service principal                                             |
| endpoint     | string | N/A     | The fully qualified endpoint of the digital twin instance                       |
| limit        | int    | 30      | The number of records to inspect to infer the schema of the data                |
| query        | string | N/A     | The query to execute against the digital twin                                   |

```scala
val df = spark.read
  .format("digitaltwin")
  .option("tenantId", "12ab34c5-1234-1ab2-123abc456de")
  .option("clientId", "12ab34c5-1234-1ab2-123abc456de")
  .option("clientSecret", "a_really_strong_password")
  .option("endpoint", "https://my-adt-instance.api.northeurope.digitaltwins.azure.net/twin-id")
  .option("query", "SELECT building, level FROM digitaltwins dt JOIN level RELATIONSHIP dt.isPartOf WHERE dt.$dtId = 'MyBuilding'")
  .load()
```

If a schema has not been defined as part of the load then it will be inferred from the result set. Note that the reader will
execute a limited version of the query first to infer the schema, and then run the full query to return the result set.

The inferred schema will combine different models, so if the relationship brings in multiple models then the returned data
frame will contain all fields.
