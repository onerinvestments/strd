package utl.core.http

import org.jboss.netty.handler.codec.http.HttpMethod
import play.api.libs.json._

/**
 *
 */
package object swagger {
  val VERSION = "1.2"


  implicit val httpMethodWrites = Writes[HttpMethod](m => JsString(m.toString))
  implicit val infoWrites = Json.writes[Info]
  implicit val resourceWrites = Json.writes[Resource]
  implicit val resourceListingWrites = Json.writes[ResourceListing]
  implicit val responseMessageWrites = Json.writes[ResponseMessage]

  implicit val parameterWrites = new DataTypeWrites[Parameter] {
    override def properties(o: Parameter): Seq[(String, JsValue)] = Seq(
      "name" -> JsString(o.name),
      "paramType" -> JsString(o.paramType.toString),
      "required" -> JsBoolean(o.required),
      "description" -> JsString(o.description.getOrElse(""))
    ) ++ super.properties(o)
  }

  implicit val propertyWrites = new DataTypeWrites[Property] {
    override def properties(o: Property): Seq[(String, JsValue)] = Seq(
      "description" -> JsString(o.description.getOrElse(""))
    ) ++ super.properties(o)
  }

  implicit val operationWrites = new DataTypeWrites[Operation] {
    override def properties(o: Operation): Seq[(String, JsValue)] = Seq(
      "nickname" -> JsString(o.nickname),
      "method" -> JsString(o.method.toString),
      "summary" -> JsString(o.summary.getOrElse("")),
      "parameters" -> JsArray(o.parameters.map(parameterWrites.writes)),
      "responseMessages" -> JsArray(o.responseMessages.map(responseMessageWrites.writes))
    ) ++ super.properties(o)
  }

  implicit val apiWrites = Json.writes[Api]
  implicit val modelWrites = Json.writes[Model]
  implicit val apiDeclarationWrites = Json.writes[ApiDeclaration]

}
