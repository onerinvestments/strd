package utl.core.http

import com.twitter.ostrich.stats.Stats
import lmbrd.zn.util.MD5Util
import org.jboss.netty.handler.codec.http.{HttpMethod, HttpResponseStatus}
import org.slf4j.LoggerFactory
import strd.util.Stats2
import utl.core.http.Router.Handler
import utl.core.http.path._

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConversions._

object Router {
  type Handler = MethodRequest => Future[MessageResponse]

  def apply(routers: List[Router]): Router = routers.size match {
    case 0 => throw new IllegalArgumentException("Empty routers")
    case 1 => routers.head
    case _ => new AggregatedRouter(routers)
  }

  def apply(router: Router, routers: Router*): Router = apply(List(router) ++ routers)
}

trait Router {
  val log = LoggerFactory.getLogger(getClass)

  def routes: List[(Method, Router.Handler)]
  lazy val compiled = routes.sortBy(_._1).reverse

  def route(http: MessageRequest) = {
    val path = http.request.getPath.stripPrefix("/").stripSuffix("/")
    compiled.view.map(
      x => (x._1, x._2, x._1.path.parse(path))
    ).find(
      x => x._1.httpMethod == http.httpMethod
        && x._3 != None
    ).map(
      x => RouteMatch(MethodRequest(x._1, http, x._3.get, http.request.getParameters.map(x => x._1 -> x._2.toList).toMap), x._2)
    )
  }
}

class AggregatedRouter(routers: List[Router]) extends Router {
  def routes: List[(Method, Handler)] = routers.
    map(_.routes).flatten. // Get list of all routes
    groupBy(_._1).toList.map(x => (x._1, x._2.last._2)) // Remove duplicates for methods: choose last handler
}

trait RoutedMessageHandler extends MessageHandler {
  implicit val executionContext: ExecutionContext

  val log = LoggerFactory.getLogger(getClass)

  def router: Router

  override def processMessage(http: MessageRequest): Future[MessageResponse] = {
    Stats2.futureTime("route/request_time") {
      Stats.incr("route/request")
      router.route(http).map(r => {
        log.trace(s"---> ${http.httpMethod} ${http.request.getPath}: ${r.req.method} ${r.req.params}")
        Stats.incr("route/request_match")
        r.handler(r.req)
      }).getOrElse({
        log.debug(s"---> ${http.httpMethod} ${http.request.getPath}: not found ")
        Stats.incr("route/request_notfound")
        notFound
      })
    }
  }

  def notFound: Future[MessageResponse] = Future {
    throw new BadRequest(HttpResponseStatus.NOT_FOUND)
  }
}

case class Method(httpMethod: HttpMethod, path: Path) extends Ordered[Method] {
  def name = MD5Util.md5asString(s"${httpMethod.toString}:${path.toString}").toLowerCase

  override def compare(that: Method) = {
    httpMethod.compareTo(that.httpMethod) match {
      case 0 => path.toString.compareTo(that.path.toString)
      case i => i
    }
  }
}
case class MethodRequest(method: Method, http: MessageRequest, params: Parameters, queryParameters: Map[String, List[String]])
case class RouteMatch(req: MethodRequest, handler: Router.Handler)
