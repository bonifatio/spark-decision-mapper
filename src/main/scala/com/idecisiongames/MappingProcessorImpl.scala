package com.idecisiongames

import com.idecisiongames.ColumnMapping.Mapping
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, count, to_date}
import org.apache.spark.sql.types.{DataType, DateType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import play.api.libs.json.Json

class MappingProcessorImpl(filePath: String) extends MappingProcessor with LazyLogging {

  def loadDataFrame(spark: SparkSession): DataFrame = {
    spark
      .read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(filePath)
  }

  def cleanseRecords(df: DataFrame): DataFrame = {
    val colNames = df.columns

    def allNonBlank(row: Row) = {
      (0 to colNames.length - 1) forall { idx =>
        Option(row.getString(idx)) forall {
          _.trim.nonEmpty
        }
      }
    }

    df filter {
      allNonBlank _
    }
  }

  def loadMappings(): Seq[Mapping] = {
    Option(getClass.getResourceAsStream("/mappings.json")) match {
      case None =>
        sys.error("Please add `mappings.json` to the app resources")

      case Some(stream) =>
        val json = try {
          Json.parse(stream)
        } finally {
          stream.close()
        }
        json.as[Seq[Mapping]]
    }
  }

  def skipColumns(
                   df: DataFrame,
                   existingColumns: Map[String, DataType],
                   mappings: Seq[Mapping]
                 ) = {

    val columnsToRemove = existingColumns.keys.toSeq diff mappings.map(_.existingName)
    df.drop(columnsToRemove: _*)
  }

  def applyMapping(existingColumns: Map[String, DataType])
                  (df: DataFrame, c: Mapping): DataFrame = {

    def rename: DataFrame = {
      if (c.newName != c.existingName) df.withColumnRenamed(c.existingName, c.newName)
      else df
    }

    def changeTypeAndRename: DataFrame = {

      val newFrame = c.newType match {
        case DateType =>
          val dateFormat = c.dateExpression.getOrElse("dd-MM-yyyy")
          df.withColumn(c.newName, to_date(col(c.existingName), dateFormat))

        case _ =>
          df.withColumn(c.newName, col(c.existingName).cast(c.newType))
      }

      if (c.newName != c.existingName) newFrame.drop(c.existingName)
      else newFrame
    }

    existingColumns.get(c.existingName) match {
      case Some(tpe) =>
        if (c.newType == tpe) rename else changeTypeAndRename

      case None =>
        logger.warn("Unknown column {}", c.existingName)
        df
    }
  }

  def getColumnStats(dataFrame: DataFrame): Seq[ColumnStats] = {
    val df = dataFrame.persist()

    def columnStats(colName: String): ColumnStats = {

      val ar = df
        .select(colName)
        .filter(r => !r.isNullAt(0))
        .groupBy(colName)
        .agg(count("*"))
        .rdd
        .map(z => z.get(0).toString -> z.getLong(1))
        .collect

      ColumnStats(
        columnName = colName,
        uniqueValues = ar.length,
        values = ar.toSeq
      )
    }

    df.columns map {
      columnStats
    }
  }
}
