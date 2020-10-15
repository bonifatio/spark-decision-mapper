package com.idecisiongames

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import play.api.libs.json.Json

object MapperApp extends LazyLogging {

  def main(args: Array[String]) {

    val config = ConfigFactory.load()
    val partitionsNumber = config.getInt("partitions-number")
    val appName = config.getString("app-name")
    val filePath = config.getString("file-path")
    val sparkMaster = config.getString("spark-master")

    val spark: SparkSession = SparkSession.builder.master(sparkMaster).appName(appName).getOrCreate()

    val mappingProcessor = new MappingProcessorImpl(filePath)
    import MappingProcessor._
    import mappingProcessor._

    val start = System.currentTimeMillis()

    val columnStats =
      for {
        originalDataFrame <- dfStep("load data frame") {
          loadDataFrame(spark).repartition(partitionsNumber)
        }
        cleansedDataFrame <- dfStep("cleanse records") { cleanseRecords(originalDataFrame) }
        existingColumns = cleansedDataFrame.schema.fields.map(f => f.name -> f.dataType).toMap
        mappings <- step("load mappings") { loadMappings() }
        selectedColumnsDf <- dfStep("select columns") {
          skipColumns(cleansedDataFrame, existingColumns, mappings)
        }
        mappedDf <- dfStep("map columns") {
          mappings.foldLeft(selectedColumnsDf)(applyMapping(existingColumns))
        }
        stats <- step("get column stats") { getColumnStats(mappedDf) }
      } yield {
        Json.toJson(stats)
      }

    columnStats.fold (
      err => println(err),
      s => println(s"=========== Column stats ===========:\n ${Json.prettyPrint(s)}")
    )

    val elapsed = (System.currentTimeMillis() - start) / 1e3

    println(s"Mapping took $elapsed seconds")

    spark.stop()
  }
}
