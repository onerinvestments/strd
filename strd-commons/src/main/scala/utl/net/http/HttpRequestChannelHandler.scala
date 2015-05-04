package utl.net.http

import java.io.{File, FileInputStream, IOException, InputStream}
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.{Timer, TimerTask}

import com.twitter.ostrich.stats.Stats
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.HttpResponseStatus._
import io.netty.handler.codec.http.{DefaultCookie, HttpHeaders, HttpResponseStatus, ServerCookieEncoder}
import org.apache.commons.io.IOUtils
import org.slf4j.{Logger, LoggerFactory}
import strd.net.util.StatSecond
import strd.trace.PoolContext
import strd.trace.PoolContext._
import strd.util.{CTime, Stats2}
import utl.core.http.ContentType

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 *
 */
object HttpRPS {
  val lastStat = new AtomicReference[StatSecond]( new StatSecond(CTime.now) )
  val log = LoggerFactory.getLogger(getClass)

  Stats.addGauge(s"server/HTTP/RPS") {
    lastStat.get().RPS
  }

  val timer = new Timer()

  timer.scheduleAtFixedRate(new TimerTask {
    var counter = 0L

    override def run() = {
      counter += 1

      val old = lastStat.getAndSet(StatSecond(CTime.now))
      if (counter % 20 == 0) {
        log.debug("HTTPServer RPS: " + old.RPS)
      }
    }
  }, 1000L, 1000L)

}
object HttpRequestChannelHandler {
  implicit val respPool = PoolContext.cachedExecutor(128, name = "respRtb")
}
abstract class HttpRequestChannelHandler extends SimpleChannelInboundHandler[HttpReq] {

  implicit val respPool = HttpRequestChannelHandler.respPool

  lazy val log = LoggerFactory.getLogger(getClass)

  var streamed: Option[HttpStream] = None

//  implicit def execctx: ExecutionContext


  def handler: HttpRequestHandler
  def streamTimeout: Int
  def scheduler: ScheduledExecutorService

  ChannelsCountStat.channels.incrementAndGet()

  def channelRead0(ctx: ChannelHandlerContext, msg: HttpReq) {
    HttpRPS.lastStat.get().count.incrementAndGet()

    if (msg.keepAlive) {
      val s = streamed.getOrElse {
        val stream = new HttpStream(ctx, streamTimeout, scheduler)
        streamed = Some(stream)
        stream
      }

      s.startRequest(msg.reqId, msg.started)
    }

    val f = try {
       handler.handle(msg)
    } catch {
      case x:Exception =>
        Future.failed(x)
    }

    f onComplete {
      case Success(r) =>
        Stats2.incr("http/" + r.status.code())
        submitResponseImpl(streamed, r, msg, ctx)
      case Failure(th : SuppressibleError) =>
        log.debug("Unexpected HTTP handler error: " + th.getMessage)
        Stats2.incr("http/502")
        submitResponseImpl(streamed, HttpResp(status = HttpResponseStatus.INTERNAL_SERVER_ERROR), msg, ctx)

      case Failure(th) =>
        log.error("Unexpected HTTP handler error", th)
        Stats2.incr("http/502")
        submitResponseImpl(streamed, HttpResp(status = HttpResponseStatus.INTERNAL_SERVER_ERROR), msg, ctx)
    }

  }

  def submitResponseImpl( f : Option[HttpStream],
                          r : HttpResp,
                          msg : HttpReq,
                          ctx : ChannelHandlerContext): Unit = {
    streamed.map { s =>
      s.appendResponse(StreamResponse(msg.reqId, r))
    }.getOrElse {
      HttpHelper.writeResponse(ctx, r)
      ctx.flush()
    }

  }

  override def channelReadComplete(ctx: ChannelHandlerContext) = {
    ctx.flush()
  }

  override def channelUnregistered(ctx: ChannelHandlerContext) = {
    ChannelsCountStat.channels.decrementAndGet()
    streamed.foreach(_.stop())
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    cause match {
      case x: IOException => log.debug( "IOError:" + cause.getMessage )
      case x: SuppressibleError => log.debug( "Error:" + cause.getMessage )
      case x => log.warn("Error", cause)
    }

    ctx.close()
  }

}

trait SuppressibleError extends Throwable {

}

trait HttpRequestHandler {
  def handle(msg: HttpReq): Future[HttpResp]
}

trait HttpNestedRequestHandler extends HttpRequestHandler {
  def handlers: Seq[OptionalRequestHandler]

  def handle(msg: HttpReq) = {
    handlers.view.flatMap(_.tryHandle(msg)).headOption.getOrElse {
      Stats2.incr("http/not_found")
      Future.successful(HttpResp(status = NOT_FOUND, body = Some(HttpHelper.responseToBuf(msg.path + " is not found"))))
    }
  }
}

trait OptionalRequestHandler {
  def tryHandle(msg: HttpReq): Option[Future[HttpResp]]
}

trait PartialPathHandler extends OptionalRequestHandler {
  type Handler = (HttpReq) => Future[HttpResp]

  def tryHandle: PartialFunction[List[String], Handler]

  def tryHandle(msg: HttpReq): Option[Future[HttpResp]] = {
    val path = msg.path.stripPrefix("/").split("/").toList
    PartialFunction.condOpt(path)(tryHandle).map(_(msg))
  }
}

trait StaticResourcesHandler {
  def log : Logger

  def classpathResource(a : List[String], msg: HttpReq) : Future[HttpResp] = {
    val path = a.mkString("/")

    log.debug(s"GET CP:$path ${msg.remoteIp} ${msg.userAgent}")

    val r = Option( Thread.currentThread().getContextClassLoader.getResourceAsStream( "static" + "/" + path ) )
    toResponse(path, r)
  }

  def fsResource(root:File, a : List[String], msg: HttpReq) : Future[HttpResp] = {
    val path = a.mkString("/")

    log.debug(s"GET FS:$path ${msg.remoteIp} ${msg.userAgent}")
    val content = new File( root, path)

    val r = if (content.exists() && content.canRead && content.isFile) {
      Some(new FileInputStream(content))
    } else {
      None
    }

    toResponse(path, r)
  }

  def toResponse(path: String, r: Option[InputStream]): Future[HttpResp] = {
    r.map { x => {
      val contentType =
        if (path.endsWith("html") || path.endsWith("htm")) {
          Some("text/html")
        } else if (path.endsWith("png")) {
          Some("image/png")
        } else if (path.endsWith("gif")) {
          Some("image/gif")
        } else if (path.endsWith("jpg")) {
          Some("image/jpeg")
        } else if (path.endsWith("js")) {
          Some("text/javascript; charset=utf-8")
        } else {
          None
        }

      val bytes = IOUtils.toByteArray(x)
      x.close()

      Future.successful(HttpResp(status = HttpResponseStatus.OK,
        contentType = contentType,
        body = Some(HttpHelper.responseToBuf(bytes))))

    }
    }.getOrElse(
        Future.successful(HttpResp(status = HttpResponseStatus.NOT_FOUND,
          body = Some(HttpHelper.responseToBuf(s"Resource: $path is not found"))))
      )
  }
}

object GlobalTrace {

  var globalTrace: Option[String] = None
  var globalTraceLevel : Int = 2

}

abstract class TraceableRequestChannelHandler extends HttpRequestChannelHandler {
  def requestTimeout: Int
  def cookieDomain: String
  def cookieMaxAge: Int

  def requestHandlers: Seq[OptionalRequestHandler]


  def streamTimeout = Option(PoolContext.get).fold(requestTimeout)(_ => 10000)

  override def channelRead0(ctx: ChannelHandlerContext, msg: HttpReq) {
    val traceCtx = fetchTraceContext(msg)

    traceOutput(1) {
      s"""
        |HttpRequest: ${msg.path}
        |IP:${msg.remoteIp}
        |Params:
        |${ msg.params.mkString(", ")}
        |Cookies:
        |${ msg.cookies.mkString("\n")}
        |Headers:
        |${ msg.headers.mkString("\n")}
        |
      """.stripMargin
    }

    super.channelRead0(ctx, msg)

    traceCtx.foreach(_ => PoolContext.clear())
  }

  def handler: HttpRequestHandler = new HttpNestedRequestHandler {
    override def handle(msg: HttpReq) = {
      val f = super.handle(msg)
      Option(PoolContext.get).foreach(ctx => {
        //PoolContext.futureStopTrace(f, ctx)

        f.onComplete {
          case Success(r) =>
            traceOutput(1) {
              s"""
                 |HttpResp: code ${r.status}
                 |ContentType: ${r.contentType}
                 |AdditionalHeaders:
                 |${r.headers.mkString("\n")}
                 |BodyLength: ${r.body.map(_.readableBytes().toString).getOrElse("Empty")}
               """.stripMargin
            }

            ctx.submitResult()

          case Failure(t) =>
            ctx.appendMessage("! Future failed, " + t.getMessage)
            ctx.submitResult()
        }

      })
      f
    }

    def handlers: Seq[OptionalRequestHandler] =  {
      val tracer = new PartialPathHandler {
        def tryHandle = {
          case "globalTrace" :: Nil => msg => {
            GlobalTrace.globalTrace = msg.params.get("id")
            msg.params.get("level").foreach(x=> GlobalTrace.globalTraceLevel = x.toInt )

            Future.successful(HttpResp(
              contentType = Some(ContentType.PLAIN),
              body = Some(HttpHelper.responseToBuf("trace:" + GlobalTrace.globalTrace))
            ))
          }

          case "trace" :: Nil => msg => {
            val level = msg.params.getOrElse("level", "1").toInt
            val id = msg.params.get("id")


            val cookie = new DefaultCookie("utl_trace", id.map(id => id + ":" + level).getOrElse(""))
            cookie.setDomain(cookieDomain)
            cookie.setPath("/")
            cookie.setMaxAge(cookieMaxAge)

            val setHeaders = Map(HttpHeaders.Names.SET_COOKIE -> Seq(ServerCookieEncoder.encode(cookie)),
              HttpHeaders.Names.CACHE_CONTROL -> Seq("no-cache,no-store,max-age=0,must-revalidate"),
              "P3P" -> Seq("CP='IDC DSP COR ADM DEVi TAIi PSA PSD IVAi IVDi CONi HIS OUR IND CNT'"))

            Future.successful(HttpResp(
              contentType = Some(ContentType.PLAIN),
              body = Some(HttpHelper.responseToBuf("trace:" + id)),
              headers = setHeaders))
          }
        }
      }

      tracer +: requestHandlers
    }
  }

  def fetchTraceContext(msg: HttpReq) = {
    val params = msg.params
    if ( msg.params.contains("traceId") ) {

      PoolContext.startTrace(params("traceId"), params.getOrElse("level", "1").toInt)

    } else if ( msg.cookies.exists { case (k, v) => k == "utl_trace" && v.contains(":") } ) {

      val cookie = msg.cookies("utl_trace")
      val sp = cookie.split(':')
      val traceId = sp(0) + "_" + CTime.now
      val level = sp(1).toInt

      log.info(s"COOKIE_TRACE IP:${msg.remoteIp} req:${msg.path} : $traceId")
      PoolContext.startTrace(traceId, level)

    } else {
      GlobalTrace.globalTrace.map { trId =>
        val traceId = trId + "_" + CTime.now
        log.info(s"GLOBAL_TRACE IP:${msg.remoteIp} req:${msg.path} : $traceId")
        PoolContext.startTrace(traceId, GlobalTrace.globalTraceLevel)
      } getOrElse {
        None
      }
    }
  }
}

object ChannelsCountStat {
  val channels = new AtomicInteger()

  Stats.addGauge("http/channels"){
    channels.get().toDouble
  }

}
