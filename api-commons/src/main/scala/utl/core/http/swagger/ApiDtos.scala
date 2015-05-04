package utl.core.http.swagger

import org.jboss.netty.handler.codec.http.HttpMethod
import play.api.libs.json._
import utl.core.http.ContentType


/**
 *
 * User: light
 * Date: 24/10/14
 * Time: 18:07
 */

object ApiDtos {

}
trait DataTypeWrites[A <: DataType] extends Writes[A] {
  override def writes(o: A): JsValue = JsObject(properties(o))

  def properties(o: A): Seq[(String, JsValue)] = Seq(
    Some("type" -> JsString(o.`type`)),
    o.format.map("format" -> JsString(_)),
    o.enum.map(e => "enum" -> JsArray(e.map(JsString))),
    o.items.map(i => "items" -> JsObject(i match {
      case ref: ModelReference => Seq("$ref" -> JsString(ref.tpe))
      case t => Seq("type" -> JsString(t.tpe), "format" -> JsString(t.format.getOrElse("")))
    }))
  ).flatten
}
case class ResourceListing(apis: Seq[Resource], apiVersion: Option[String], info: Option[Info] = None, swaggerVersion: String = VERSION)

case class Info(title: String, description: String)

case class Resource(path: String, description: Option[String])

case class ApiDeclaration(basePath: String, resourcePath: String, apis: Seq[Api], models: Map[String, Model]) {
  val swaggerVersion = VERSION
  val produces, consumes = Seq(ContentType.JSON)
}

case class Api(path: String, operations: Seq[Operation], description: Option[String])

object ParameterType extends Enumeration {
  val PATH    = Value("path")
  val QUERY   = Value("query")
  val BODY    = Value("body")
  val HEADER  = Value("header")
  val FORM    = Value("form")
}

object Parameter {
  val BODY = "body"

  def body(fieldType: FieldType, required: Boolean, description: Option[String] = None) =
    apply(BODY, ParameterType.BODY, fieldType: FieldType, required: Boolean, description: Option[String])
}
case class Parameter(name: String, paramType: ParameterType.Value, fieldType: FieldType, required: Boolean, description: Option[String] = None)
  extends DataType(fieldType)

case class Operation(nickname: String, method: HttpMethod, parameters: Seq[Parameter], fieldType: FieldType, responseMessages: Seq[ResponseMessage], summary: Option[String])
  extends DataType(fieldType)

case class ResponseMessage(code: Int, message: String, responseModel: Option[String] = None)

case class Model(id: String, properties: Map[String, Property], required: Seq[String], description: Option[String] = None)

case class Property(fieldType: FieldType, description: Option[String] = None) extends DataType(fieldType)

sealed abstract class DataType(fieldType: FieldType) {
  val `type` = fieldType.tpe
  val format = fieldType.format
  val enum = fieldType.enum
  val items = fieldType.items
}

sealed trait FieldType {
  def tpe: String
  def format: Option[String] = None
  def enum: Option[Seq[String]] = None
  def items: Option[Items] = None
}
sealed trait Items extends FieldType

object Primitive extends Enumeration {
  case class TypeValue(name: String, tpe: String, override val format: Option[String] = None) extends Val(name) with Items {}

  val INT       = TypeValue("integer", "integer", Some("int32"))
  val LONG      = TypeValue("long", "integer", Some("int64"))
  val FLOAT     = TypeValue("float", "number", Some("float"))
  val DOUBLE    = TypeValue("double", "number", Some("double"))
  val STRING    = TypeValue("string", "string")
  val BYTE      = TypeValue("byte", "string", Some("byte"))
  val BOOL      = TypeValue("boolean", "boolean")
  val DATE      = TypeValue("date", "string", Some("date"))
  val DATE_TIME = TypeValue("dateTime", "string", Some("date-time"))
  val VOID      = TypeValue("void", "void")
}

case class ArrayField(subType: Items) extends FieldType {
  val tpe = "array"
  override def items = Some(subType)
}

case class ModelReference(tpe: String) extends Items

case class Enum(values: Seq[String]) extends Items {
  override def tpe: String = Primitive.STRING.tpe

  override def enum: Option[Seq[String]] = Some(values)
}
