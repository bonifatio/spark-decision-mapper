package com.idecisiongames

import com.idecisiongames.ColumnMapping.Mapping
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

trait MappingProcessor {

  def loadDataFrame(spark: SparkSession): DataFrame

  def cleanseRecords(df: DataFrame): DataFrame

  def loadMappings(): Seq[Mapping]

  def skipColumns(
                   df: DataFrame,
                   existingColumns: Map[String, DataType],
                   conversions: Seq[Mapping]
                 ): DataFrame

  def applyMapping(existingColumns: Map[String, DataType])
                  (df: DataFrame, c: Mapping): DataFrame

  def getColumnStats(dataFrame: DataFrame): Seq[ColumnStats]
}

object MappingProcessor {

  def step[T](actionName: String)(block: => T) = {
    Try { block }
      .toEither
      .left
      .map { ex =>
        s"Failed to $actionName; ${ex.getMessage}"
      }
  }

  def dfStep(actionName: String, showResult: Boolean = true)(block: => DataFrame) = {
    step(actionName)(block) map { df =>
      if (showResult) df.show()
      df
    }
  }
}
