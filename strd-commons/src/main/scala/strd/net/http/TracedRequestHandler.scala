package strd.net.http

import io.netty.handler.codec.http._
import lmbrd.zn.util.TimeUtil
import org.slf4j.LoggerFactory
import strd.net.http.matching._
import strd.trace.PoolContext
import strd.util.CTime
import utl.net.http.HttpHelper

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * @author Kirill chEbba Chebunin
 */
class TracedRequestHandler(parent       : RequestHandler,
                           cookieDomain : String,
                           cookieMaxAge : Int = TracedRequestHandler.COOKIE_MAX_AGE)
                          (implicit execctx: ExecutionContext) extends RequestHandler {

  val log = LoggerFactory.getLogger(getClass)

  var globalTrace: Option[String] = None
  var globalTraceLevel : Int = 2

  def handle(req: HttpReq) = {
    val traceCtx = fetchTraceContext(req)

    traceCtx.foreach(_.appendMessage(s"HttpRequest: $req"))

    val resp = req match {
      case GET -> "trace" => setTraceCookie(req)
      case GET -> "globalTrace" => setGlobalTrace(req)
      case _ => parent.handle(req)
    }

    traceCtx.foreach { ctx =>
      resp.onComplete {
        case Success(r) =>
          ctx.appendMessage(s"HttpRequest ${req.id} response: $r")
          ctx.submitResult()

        case Failure(e) =>
          ctx.appendMessage(s"HttpRequest ${req.id} failed: ${e.getMessage}")
          ctx.submitResult()
      }
    }

    traceCtx.foreach(_ => PoolContext.clear())

    resp
  }

  def fetchTraceContext(req: HttpReq) = {
    req.query.getOne("traceId").map { traceId =>
      val level = req.query.getOneOrElse("level", "1").toInt
      log.info(s"QUERY_TRACE $traceId:$level for ${req.uri} from IP ${req.ip}")
      PoolContext.startTrace(traceId, level)
    }.orElse {
      req.cookies.get("utl_trace").map { v =>
        val sp = v.split(':')
        val traceId = sp(0) + "_" + CTime.now
        val level = sp.drop(1).headOption.getOrElse("1").toInt

        log.info(s"COOKIE_TRACE $traceId:$level for ${req.uri} from IP ${req.ip}")
        PoolContext.startTrace(traceId, level)
      }
    }.orElse {
      globalTrace.map { trId =>
        val traceId = trId + "_" + CTime.now
        log.info(s"GLOBAL_TRACE $traceId:$globalTrace for ${req.uri} from IP ${req.ip}")
        PoolContext.startTrace(traceId, globalTraceLevel)
      }
    }.getOrElse {
      None
    }
  }

  def setTraceCookie(req: HttpReq) = {
    val id = req.query.getOne("id")
    val level = req.query.getOneOrElse("level", "1").toInt

    val trace = id.map(_ + ":" + level).getOrElse("")

    val cookie = Cookie("utl_trace", trace, domain = Some(cookieDomain), maxAge = Some(cookieMaxAge))

    Future.successful(HttpResp(
      body = Some(Content("trace:" + trace)),
      headers = Map(
        ContentType(_.PLAIN),
        Cookies(cookie),
        NoCache
      )
    ))
  }
  
  def setGlobalTrace(req: HttpReq) = {
    globalTrace = req.query.getOne("id")
    req.query.getOne("level").foreach(x=> globalTraceLevel = x.toInt )

    Future.successful(HttpResp(
      body = Some(HttpHelper.responseToBuf("trace:" + globalTrace)),
      headers = Map(
        HttpHeaders.Names.CONTENT_TYPE  -> Seq(ContentType.PLAIN)
      )
    ))
  }
}

object TracedRequestHandler {
  val COOKIE_MAX_AGE = (365L * TimeUtil.aDAY / 1000L).toInt
}
