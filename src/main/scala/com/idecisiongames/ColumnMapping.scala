package com.idecisiongames

import org.apache.spark.sql.types._
import play.api.libs.json._

object ColumnMapping {

  final case class Mapping(
      existingName: String,
      newName: String,
      newType: DataType,
      dateExpression: Option[String] = None
  )

  object Mapping {
    implicit val DataTypeReads = new Reads[DataType] {
      override def reads(json: JsValue): JsResult[DataType] = {
        def handleTypeId: PartialFunction[String, DataType] = {
          case "integer" => IntegerType
          case "date"    => DateType
          case "boolean" => BooleanType
          case "string"  => StringType
        }

        for {
          typeCode <- json.validate[String]
          dataType <- handleTypeId.lift(typeCode) match {
            case Some(t) => JsSuccess(t)
            case None    => JsError(s"Type '$typeCode' is not supported")
          }
        } yield {
          dataType
        }
      }
    }

    implicit val conversionReads: Reads[Mapping] = (json: JsValue) => {
      for {
        existingName <- (json \ "existing_col_name").validate[String]
        newName <- (json \ "new_col_name").validate[String]
        newType <- (json \ "new_data_type").validate[DataType]
        dateExpression <- (json \ "date_expression").validateOpt[String]
        _ <- if ((newType == DateType) && dateExpression.isEmpty)
          JsError("Missed date format")
        else JsSuccess(())
      } yield {
        Mapping(existingName, newName, newType, dateExpression)
      }
    }

  }

}
