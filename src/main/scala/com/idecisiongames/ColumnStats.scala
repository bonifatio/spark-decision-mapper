package com.idecisiongames

import play.api.libs.json.{JsNumber, JsValue, Json, Writes}

final case class ColumnStats(
                              columnName: String,
                              uniqueValues: Long,
                              values: Seq[(String, Long)]
                            )
object ColumnStats {

  implicit val ColumnStatsFormat = new Writes[ColumnStats] {
    def writes(o: ColumnStats): JsValue = {

      Json.obj(
        "Column" -> o.columnName,
        "Unique_values" -> o.uniqueValues,
        "Values" -> o.values. map { case(v, n) => Json.obj(v -> JsNumber(n)) }
      )
    }
  }
}
