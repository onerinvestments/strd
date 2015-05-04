package utl.core.http

import java.text.SimpleDateFormat
import java.util.Date

import com.fasterxml.jackson.core.JsonProcessingException
import org.jboss.netty.handler.codec.http.HttpMethod
import play.api.libs.json._
import utl.core.http.Router.Handler
import utl.core.http.path._
import utl.core.http.swagger._

import scala.collection.convert.Wrappers.{JListWrapper, JMapWrapper}
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe._

case class ApiRequest[I] (data: I, params: Parameters, queryParameters: Map[String, List[String]], msg: MessageRequest) {
  def param[A](name: String) = params(name).asInstanceOf[A]
  def paramOpt[A](name: String) = params.get(name).map(v => v.asInstanceOf[A])
}

object ResponseStatus extends Enumeration {
  case class Status(name: String) extends Val(name)
  /** success */
  val SUCCESS = Status("success")
  /** error */
  val ERROR   = Status("error")

  implicit val statusWrites: Writes[Status] = Writes(status => JsString(status.name))
  implicit val statusReads: Reads[Status] = Reads(value => value.as[String] match {
    case "success" => JsSuccess(SUCCESS)
    case "error" => JsSuccess(ERROR)
    case smth => throw new IllegalArgumentException(s"Failed to deserialize value $smth")
  })

}

sealed trait ApiResponse[+O] {
  def status: ResponseStatus.Status
  def get: O
}

case class ApiSuccess[O](result: O) extends ApiResponse[O] {
  val status = ResponseStatus.SUCCESS
  def get = result
}

object ApiError {
  val UNKNOWN = apply(-1, "Unknown Error")
  val INVALID_INPUT = apply(1, "Wrong input format")
  val INVALID_PARAMETERS = apply(InvalidParameters, "Invalid parameters")
}

case class ApiError(code: Int, message: String) extends ApiResponse[Nothing] {
  val status = ResponseStatus.ERROR
  def get = throw new NoSuchElementException("ApiError.get")
}

case class ApiException(code: ErrorCode, message: String) extends RuntimeException(message)

object InvalidParameters extends ErrorCode(400) {
  def apply(m: String) = ApiException(this, m)
}

object ApiRouter {
  val ALLOW_API = Map(
    "Access-Control-Allow-Origin"   -> Seq("*"),
    "Access-Control-Allow-Methods"  -> Seq("GET, POST, DELETE, PUT"),
    "Access-Control-Allow-Headers"  -> Seq("Content-Type, api_key, Authorization")
  )

  def messageResponse(content: String) = {
    MessageResponse(Some(content), contentType = ContentType.JSON, headers = ALLOW_API)
  }
}

trait ApiRouter extends Router with ApiDoc {

  implicit def executionContext: ExecutionContext

  implicit def responseWrites[O](implicit writes: Writes[O]): Writes[ApiResponse[O]] = Writes { res: ApiResponse[O] =>
    JsObject(Seq[(String, JsValue)](
      "status" -> JsString(res.status.name),
      "result" -> (res match {
        case ApiSuccess(_) => writes.writes(res.get)
        case e: ApiError => Json.writes[ApiError].writes(e)
      })
    ))
  }
  implicit def unitWrites: Writes[Unit] = Writes(_ => JsObject(Seq.empty))
  implicit def unitReads: Reads[Unit] = Reads(_ => JsSuccess(Unit))

  var methods: List[(OperationDefinition, Handler)] = List.empty

  def routes: List[(Method, Handler)] = methods.map(m => (m._1.method, m._2)) :+ apiRoute

  def operations: List[OperationDefinition] = methods.map(_._1)

  def bindRequest[I](req: MethodRequest)(implicit in: TypeTag[I], reads: Reads[I]) = in.tpe match {
    case u if u =:= typeOf[Unit] => ApiRequest(Unit.asInstanceOf[I], req.params, req.queryParameters, req.http)
    case o if o <:< typeOf[Option[Any]] => ApiRequest(
      if (!req.http.content.isEmpty) parseRequest[I](req) else None.asInstanceOf[I],
      req.params,
      req.queryParameters,
      req.http
    )
    case _ => ApiRequest(parseRequest[I](req), req.params, req.queryParameters, req.http)
  }

  val formatter = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss")

  def parseRequest[I](req: MethodRequest)(implicit in: TypeTag[I], reads: Reads[I]) = {
    val json = req.http.httpMethod match {
      case HttpMethod.GET | HttpMethod.DELETE =>
        val map = JMapWrapper(req.http.request.getParameters).map { x =>
          if (x._1.endsWith("[]")) {
            x._1.substring(0, x._1.size - 2) -> Json.toJson(JListWrapper(x._2).toSeq)
          } else {
            val fieldName: String = x._1
            val str = JListWrapper(x._2).toSeq.headOption
            val signature: Type = in.tpe.members.find(mem => !mem.isMethod && mem.name.toString == fieldName+" ").get.typeSignature

            if (signature =:= typeOf[Int] || signature =:= typeOf[Option[Int]]) {
              fieldName -> Json.toJson(str.map(_.toInt))
            } else if (signature =:= typeOf[Boolean] || signature =:= typeOf[Option[Boolean]]) {
              fieldName -> Json.toJson(str.map(_.toBoolean))
            } else if (signature =:= typeOf[Date] || signature =:= typeOf[Option[Date]]) {
              fieldName -> Json.toJson(str.map(formatter.parse))
            } else {
              fieldName -> Json.toJson(str)
            }
          }
        }
        Json.toJson(JsObject(map.toSeq))
      case HttpMethod.POST | HttpMethod.PUT | HttpMethod.PATCH => Json.parse(req.http.content)
      case m => throw new IllegalArgumentException(s"Unsupported method $m")
    }

    json.as[I](reads)
  }

  def response[O](res: ApiResponse[O])(implicit writes : Writes[O]) = {
    ApiRouter.messageResponse(Json.toJson(res)(responseWrites[O]).toString())
  }

  val POST    = MethodBuilder(this, HttpMethod.POST, Path())
  val PUT     = MethodBuilder(this, HttpMethod.PUT, Path())
  val GET     = MethodBuilder(this, HttpMethod.GET, Path())
  val DELETE  = MethodBuilder(this, HttpMethod.DELETE, Path())

  type ApiHandler[I, O] = ApiRequest[I] => Future[ApiResponse[O]]

  implicit def handlerToCallback[I,O](handler: ApiHandler[I,O]) = Callback(handler)

  implicit def dataCallback[I,O](handler: ApiRequest[I] => Future[O]) = handlerToCallback((req: ApiRequest[I]) => {
    handler(req).map(ApiSuccess(_)).recover {// TODO: Log
      case e: ApiException =>
        log.debug("API exception", e)
        ApiError(e.code.value, e.message)
      case t =>
        log.warn("Unexpected handler exception", t)
        ApiError.UNKNOWN
    }
  })

  case class QueryParameter(name: String, dataType: FieldType)

  def intParam(name: String) = QueryParameter(name, Primitive.INT)
  def strParam(name: String) = QueryParameter(name, Primitive.STRING)

  case class MethodBuilder(router: ApiRouter, httpMethod: HttpMethod, path: Path) {
    def /(s: Segment) = MethodBuilder(router, httpMethod, path / s)
    def :?(q: QueryParameter) = MethodParamBuilder(this, Seq(q))
    def &(q: QueryParameter) = MethodParamBuilder(this, Seq(q))
    def will(desc: String)  = MethodHandlerBuilder(MethodParamBuilder(this), desc)
  }
  case class MethodParamBuilder(mb: MethodBuilder, params: Seq[QueryParameter] = Seq.empty) {
    def will(desc: String)  = MethodHandlerBuilder(this, desc)
    def &(q: QueryParameter) = MethodParamBuilder(mb, params :+ q)
  }

  case class MethodHandlerBuilder(dmb: MethodParamBuilder, desc: String) {
    def :=[I, O](callback: Callback[I,O])(implicit out: TypeTag[O], writes: Writes[O], in: TypeTag[I], reads: Reads[I]) = {
      val handler = {req: MethodRequest => {
        val future = try {
          callback.handler(bindRequest[I](req)).recover {
            case t: Exception =>
              log.error("Api method future exception", t)
              ApiError.UNKNOWN
          }
        } catch {
          case e: JsonProcessingException =>
            log.debug("Json processing exception", e)
            Future(ApiError.INVALID_INPUT)
          case e: JsResultException =>
            log.debug("Json mapping exception", e)
            Future(ApiError.INVALID_PARAMETERS)
          case t: Exception =>
            log.warn("Request parsing exception", t)
            Future(ApiError.UNKNOWN)
        }

        future.map(response(_))
      }}

      methods :+= (OperationDefinition(Method(dmb.mb.httpMethod, dmb.mb.path), in.tpe, out.tpe, callback.errors, desc) -> handler)
    }
  }

  case class Callback[I,O](handler: ApiHandler[I,O], errors: Map[ErrorCode, String] = Map.empty) {
    def errors(errors: (ErrorCode, String)*) = Callback(handler, errors.toMap)
  }

  def apiRoute = Method(HttpMethod.GET, docPath) -> {req: MethodRequest => Future {
    ApiRouter.messageResponse(Json.toJson(declaration(req.http.baseUrl)).toString())
  }}

}

class DocumentedApiRouter(routers: List[ApiRouter])(implicit val executionContext: ExecutionContext) extends AggregatedRouter(routers) {
  def apidoc = ResourceListing(routers.map(r => Resource(r.basePath.toString, None)), apiVersion = Some("1.0"))

  override def routes: List[(Method, Handler)] = super.routes :+ (Method(HttpMethod.GET, "api-docs") -> {req: MethodRequest => Future {
    ApiRouter.messageResponse(Json.toJson(apidoc).toString())
  }})
}
