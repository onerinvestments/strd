package utl.core.http

import java.util.concurrent.ConcurrentHashMap

import com.twitter.ostrich.admin.Service
import org.jboss.netty.handler.codec.http.HttpMethod
import utl.core.http.path._
import utl.core.http.swagger._

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

object ErrorCode {
  implicit def toInt(c: ErrorCode): Int = c.value
}
case class ErrorCode(value: Int)

trait ApiDoc extends Service {

  type Errors = Map[ErrorCode, String]

  def basePath = operations.map(_.method.path).foldLeft[Option[Path]](None)((base, path) => base match {
    case Some(x) => Some(x.base(path))
    case None => Some(path)
  }).map(_.staticPrefix).getOrElse(Path())

  def docPath: Path = Path("api-docs") + basePath

  def operations: List[OperationDefinition]

  var models: Seq[Model] = null

  var apis: Seq[Api] = null

  def declaration(endpoint: String = "/") = ApiDeclaration(
    endpoint,
    basePath.toString,
    apis,
    models.toList.map(m => (m.id, m)).toMap
  )

  override def start() {
    models = operations.flatMap(d => d.models).distinct.sortBy(_.id)
    apis = operations.groupBy(_.method.path).toList.map(a => Api(a._1.toString, a._2.map(_.operation).sortBy(_.method), None)).sortBy(_.path)
  }

  override def shutdown() {

  }

  case class OperationDefinition(method: Method, in: Type, out: Type, errors: Errors, desc: String) {

    def models: Seq[Model] = List(in, out).flatMap(typeModels).distinct

    def operation = Operation(
      method.name,
      method.httpMethod,
      parameters,
      ModelType(out).fieldType,
      responseMessages,
      Some(desc)
    )

    def parameters: Seq[Parameter] =
      pathParameters ++ (method.httpMethod match {
        case HttpMethod.GET | HttpMethod.DELETE => queryParameters
        case _ => List(bodyParameter).flatten
      })

    def responseMessages: Seq[ResponseMessage] = errors.toList.map(e => ResponseMessage(e._1.value, e._2))

    def responseModel: Option[String] = ModelType(out) match {
      case u if u.fieldType == Primitive.VOID => None
      case m => Some(m.name)
    }

    def errorMessages: Seq[ResponseMessage] = errors.toList.map(e => ResponseMessage(e._1.value, e._2))

    def typeModels(tpe: Type): Seq[Model] = {
      val main = new ModelType(tpe)

      (main.fieldType match {
        case m: ModelReference =>
          List(Model(
            main.name,
            main.properties.map(p => (p._1, Property(p._2.fieldType))),
            main.properties.flatMap(p => p._2.tpe match {
              case opt if opt <:< typeOf[Option[Any]] => None
              case _ => Some(p._1)
            }).toSeq
          )) ++
          main.properties.toList.flatMap(p => p._2.fieldType match {
            case _: ModelReference => typeModels(p._2.tpe)
            case _ => List.empty
          })

        case _ => List()

      }) ++ subTypes(tpe).flatMap(typeModels)
    }

    def subTypes(tpe: Type) = tpe match {
      case TypeRef(_, _, args) => args
      case _ => List.empty
    }

    def pathParameters: Seq[Parameter] = method.path.parameters.map(p => Parameter(
      p.name,
      ParameterType.PATH,
      p match {
        case int(_) => Primitive.INT
        case long(_) => Primitive.LONG
        case enum(_, e) => Enum(e.values.toSeq.map(_.toString))
        case _ => Primitive.STRING
      },
      required = true,
      None
    ))

    def bodyParameter: Option[Parameter] = in match {
      case u if u =:= typeOf[Unit] => None
      case o if o <:< typeOf[Option[Any]] =>
        Option(Parameter.body(ModelType(o).fieldType, required = false))
      case m =>
        Option(Parameter.body(ModelType(m).fieldType, required = true))
    }

    def queryParameters: Seq[Parameter] = ModelType(in).properties.toSeq.map(p => Parameter(
      p._1,
      ParameterType.QUERY,
      p._2.fieldType,
      !(p._2.tpe <:< typeOf[Option[Any]])
    ))
  }

  object ModelType {
    val cache = new ConcurrentHashMap[Type, ModelType]().asScala

    def apply(tpe: Type) = cache.getOrElseUpdate(tpe, new ModelType(tpe))
  }
  class ModelType(val tpe: Type) {
    def name = tpe.typeSymbol.name.toString

    lazy val fieldType: FieldType = fieldType(tpe)

    lazy val properties: Map[String, ModelType] = properties(tpe)

    def properties(t: Type): Map[String, ModelType] = t match {
      case o if o <:< typeOf[Option[Any]] =>
        properties(o.asInstanceOf[TypeRef].args.head)
      case r if fieldType(r).isInstanceOf[ModelReference] =>
        r.members.toSeq.collect {
          case m: MethodSymbol if m.isCaseAccessor => m
        }.map {s =>
          (s.name.toString, new ModelType(s.asMethod.returnType))
        }.toMap

      case _ => Map.empty
    }

    def fieldType(t: Type): FieldType = t.normalize match {
      case p if p =:= typeOf[Int] => Primitive.INT
      case p if p =:= typeOf[Long] => Primitive.LONG
      case p if p =:= typeOf[Boolean] => Primitive.BOOL
      case p if p =:= typeOf[Float] => Primitive.FLOAT
      case p if p =:= typeOf[Double] => Primitive.DOUBLE
      case p if p =:= typeOf[Unit] => Primitive.VOID
      case p if p <:< typeOf[AnyVal] => Primitive.STRING
      case p if p =:= typeOf[java.lang.String] => Primitive.STRING
      case p if p =:= typeOf[BigDecimal] => Primitive.DOUBLE
      case e if e <:< typeOf[Enumeration#Value] => Enum(
        runtimeMirror(getClass.getClassLoader)
          .reflectModule(e.asInstanceOf[TypeRef].pre.termSymbol.asModule).instance.asInstanceOf[Enumeration]
          .values.toSeq.map(_.toString)
      )
      case c if c <:< typeOf[scala.collection.Traversable[Any]] => ArrayField(
        fieldType(c.asInstanceOf[TypeRef].args.head) match {
          case f: Items => f
          case x => throw new IllegalStateException(s"ArrayField can not contain $x fields")
        }
      )
      case o if t <:< typeOf[Option[Any]] => fieldType(o.asInstanceOf[TypeRef].args.head)
      case x => ModelReference(x.typeSymbol.name.toString)
    }
  }
}
