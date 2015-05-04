package utl.core.http

import java.net.InetSocketAddress
import java.nio.channels.ClosedChannelException
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Date, Locale}

import com.twitter.ostrich.stats.Stats
import io.netty.buffer.Unpooled
import io.netty.handler.codec.base64.Base64
import io.netty.handler.codec.http
import org.apache.commons.lang.time.{DateFormatUtils, DateUtils}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel.{Channel, _}
import org.jboss.netty.handler.codec.http.{HttpHeaders, _}
import org.jboss.netty.util.CharsetUtil
import org.slf4j.LoggerFactory

import scala.collection.convert.decorateAll._
import scala.concurrent._
import scala.util.{Failure, Success}


/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 5/7/13
 * Time: 4:34 PM
 */
@deprecated("Use netty 4 handlers", "utl.net.*")
class HttpHandler(val handler: MessageHandler) extends SimpleChannelHandler {

  import scala.concurrent.ExecutionContext.Implicits.global

  val log = LoggerFactory.getLogger(getClass)

  val channels = new AtomicInteger()


  def getRemoteAddr(ch: Channel, http: HttpRequest) = {
    val ip = Option(http.getHeader("X-Real-IP")).orElse(Option(http.getHeader("X-Forwarded-For"))).orElse(Option(
      ch.getRemoteAddress.asInstanceOf[InetSocketAddress].getAddress.getHostAddress
    )).map(
      ip => if(ip.contains(",") || ip == "unknown")
        ch.getRemoteAddress.asInstanceOf[InetSocketAddress].getAddress.getHostAddress
        else ip
    )

    ip.getOrElse(throw new RuntimeException("can not fetch ip"))
  }

  val chLocal = new ChannelLocal[Long]()

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    channels.incrementAndGet()

    chLocal.set(ctx.getChannel, System.currentTimeMillis())

    if (log.isTraceEnabled)
      log.trace("connected: " + ctx.getChannel.getRemoteAddress)
  }

//  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
//    channels.incrementAndGet()
//  }

  Stats.addGauge("openChannels"){
    channels.get().toDouble
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    channels.decrementAndGet()
    Stats.addMetric("channel/alive", (System.currentTimeMillis() - chLocal.remove(ctx.getChannel)).toInt )
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {

    val channel = e.getChannel
    val httpRequest = e.getMessage.asInstanceOf[HttpRequest]

    if (log.isTraceEnabled) {
      log.trace(s"Access: ${ctx.getChannel.getRemoteAddress}   ${httpRequest.getMethod.getName} ${httpRequest.getUri}")
    }

    val ip = getRemoteAddr(channel, httpRequest)

    if (httpRequest.getUri.contains("crossdomain.xml")) {
      writeOutput(channel, MessageResponse(Some(ContentType.crossDomain), ContentType.XML))
      return
    }

    if (httpRequest.getUri.contains("robots.txt")) {
      writeOutput(channel, MessageResponse(Some(ContentType.robots), ContentType.PLAIN))
      return
    }

    if (httpRequest.getUri.contains("favicon.ico")) {
      sendHttpError(channel, HttpResponseStatus.NOT_FOUND, "")
      return
    }
    //channel.getRemoteAddress.asInstanceOf[InetSocketAddress].getAddress

    parseRequest(httpRequest, ip).onComplete {
      case Success(content) =>
        writeOutput(channel, content)

      case Failure(t) => t match {

/*
        case t: SystemException =>
          log.error(t.toString(), t)
          sendHttpError(channel, HttpResponseStatus.BAD_REQUEST, t.toString())
*/

        case t: BadRequest =>
          t.msg.foreach(msg => log.trace(s"BAD_REQ: ${httpRequest.getUri} ,message: $msg"))
          sendHttpError(channel, t.status, t.msg.getOrElse(""))

        case t: Exception =>
          log.error("process request error", t)
          sendHttpError(channel, HttpResponseStatus.INTERNAL_SERVER_ERROR, if (t.getMessage == null) t.getClass.getName else t.getMessage)
      }

    }

  }


  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {

    e.getCause match {
      case x: ClosedChannelException =>

      case t =>
        log.warn("Unexpected exception", t)
        ctx.getChannel.close()
    }

  }

  def writeOutput(channel: Channel, content: MessageResponse) {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, content.code)
    if (content.code == HttpResponseStatus.FOUND) {
      response.setHeader(HttpHeaders.Names.LOCATION, content.body.get)
    } else {
      content.body.foreach(x => response.setContent(ChannelBuffers.copiedBuffer(x, CharsetUtil.UTF_8)))
      content.raw.foreach(x => response.setContent(x))

      response.setHeader(HttpHeaders.Names.CONTENT_TYPE, content.contentType)
    }

//    if (!content.cookies.isEmpty) {
//      val encoder = new CookieEncoder(true)
//      content.cookies.foreach(c => encoder.addCookie(c))
//
//      response.setHeader("Set-Cookie", encoder.encode())
//    }

    writeAndClose(channel, response, content.headers, content.disableCache)
  }

  def sendHttpError(channel: Channel, status: HttpResponseStatus, body: String) {
    val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status)
    response.setContent(ChannelBuffers.copiedBuffer(body, CharsetUtil.UTF_8))
    writeAndClose(channel, response)
  }

  def writeAndClose(channel: Channel, response: HttpResponse, headers: Map[String, Seq[String]] = Map.empty, disableCache: Boolean = true) {
    setDefaultHeaders(response, disableCache)

    if (response.getStatus.getCode != HttpResponseStatus.NO_CONTENT.getCode ) {
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(response.getContent.readableBytes()))
      response.setHeader("P3P", "CP='IDC DSP COR ADM DEVi TAIi PSA PSD IVAi IVDi CONi HIS OUR IND CNT'")
    }

    headers.foreach { case (header, value) => response.setHeader(header, value.asJava) }

    channel.write(response).addListener(ChannelFutureListener.CLOSE)
  }

  private def setDefaultHeaders(response: HttpResponse, disableCache: Boolean = true) {
    response.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE)

    if (!disableCache) {
      response.setHeader(HttpHeaders.Names.CACHE_CONTROL, "max-age=31556926")
    } else if (response.getStatus.getCode != HttpResponseStatus.NO_CONTENT.getCode ) {
      response.setHeader(HttpHeaders.Names.CACHE_CONTROL, HttpHeaders.Values.NO_STORE)
      response.setHeader(HttpHeaders.Names.PRAGMA, HttpHeaders.Values.NO_CACHE)
      response.setHeader(HttpHeaders.Names.EXPIRES, DateFormatUtils.formatUTC(DateUtils.addSeconds(new Date, 0), "EEE, dd MMM yyyy kk:mm:ss z", Locale.US))
      response.setHeader(HttpHeaders.Names.DATE, DateFormatUtils.formatUTC(new Date, "EEE, dd MMM yyyy kk:mm:ss z", Locale.US))
    }

  }

  def parseRequest(req: HttpRequest, ip: String) = {
//    val startTime = Platform.currentTime
    val queryStringDecoder = new QueryStringDecoder(req.getUri, CharsetUtil.UTF_8)

    val header = Option(req.getHeader("Cookie"))

    val cookies = header.map(h => http.CookieDecoder.decode(h).asScala).toSeq.flatten
    val path = queryStringDecoder.getPath
    val referrer = Option(req.getHeader(HttpHeaders.Names.REFERER))

    val idx = path.lastIndexOf("/")
    if (idx < 0) throw new IllegalArgumentException("empty method name")

    val method = path.substring(idx + 1)
    val message = req.getContent.toString(CharsetUtil.UTF_8)

    if ( log.isTraceEnabled ) {
      log.debug(s"call method $method, message $message, cookies: ${cookies.mkString(":")}")
    }

    val userAgent = Option(req.getHeader(HttpHeaders.Names.USER_AGENT)).getOrElse("")
    val future = handler.processMessage(MessageRequest(req, queryStringDecoder, method, message, ip, cookies, userAgent, referrer))
/*
    future.onComplete{
      case Success(resp) => Stats.addMetric(s"http/$method/${resp.code.getCode}", Platform.currentTime - startTime toInt)
      case Failure(t) =>  Stats.addMetric(s"http/$method/${t.getClass.getName}", Platform.currentTime - startTime toInt)
    }*/

    future
  }

}

/** @deprecated("Use netty 4 handlers", "utl.net.*") */
case class MessageRequest(req: HttpRequest,
                          request: QueryStringDecoder,
                          method: String,
                          content: String,
                          remoteIp: String,
                          cookies: Iterable[http.Cookie],
                          userAgent: String,
                          referrer: Option[String] ) {
  def parameterOption(name: String) = Option(request.getParameters.get(name)).flatMap(list => if (list.isEmpty) None else Some(list.get(0)))

  def parameter(name: String) = parameterOption(name).getOrElse(throw new BadRequest(HttpResponseStatus.BAD_REQUEST, Some(s"parameter $name is required")))
  def parameterSeq(name: String) = Option(request.getParameters.get(name)).map(_.asScala.toIndexedSeq ).getOrElse(Nil)

  def httpMethod = req.getMethod

  def host = Option(HttpHeaders.getHost(req))
  def baseUrl = host.map("http://" + _).getOrElse("/")
}

/** @deprecated("Use netty 4 handlers", "utl.net.*") */
case class MessageResponse(body: Option[String],
                           contentType: String = ContentType.PLAIN,
                           headers: Map[String, Seq[String]] = Map.empty,
                           disableCache: Boolean = true,
                           code: HttpResponseStatus = HttpResponseStatus.OK,
                           raw: Option[ChannelBuffer] = None)

case class BadRequest(status: HttpResponseStatus, msg: Option[String] = None) extends Exception

@deprecated("Use netty 4 handlers", "utl.net.*")
trait MessageHandler {

  val logger = LoggerFactory.getLogger(getClass)

  def processMessage(http: MessageRequest): Future[MessageResponse]

  def authAsync(http: MessageRequest, usersPass: Seq[String], process: MessageRequest => Future[MessageResponse])(implicit executionContext: ExecutionContext): Future[MessageResponse] = {


    val authHeaders = http.req.getHeaders(HttpHeaders.Names.AUTHORIZATION)

    if (authHeaders.isEmpty) {
      logger.debug(s"not authorized, method: ${http.method}, ip: ${http.remoteIp}")
      Future(MessageResponse(Some("not authorized"), code = HttpResponseStatus.UNAUTHORIZED, headers = Map(HttpHeaders.Names.WWW_AUTHENTICATE -> Seq("Basic realm=\"uptolike gateway\""))))
    } else {
      val header = authHeaders.get(0)

      if (header.contains("Basic ")) {
        val auth = header.replace("Basic ", "")
        val userPass = Base64.decode(Unpooled.wrappedBuffer(auth.getBytes)).toString(Charset.forName("UTF8"))
        if (usersPass.contains(userPass)) {
          logger.debug(s"method: ${http.method}, user ${userPass.takeWhile(_ != ':')}, ip: ${http.remoteIp}")
          process(http)
        } else {
          logger.debug(s"method: ${http.method}, bad user password ${userPass.takeWhile(_ != ':')}, ip: ${http.remoteIp}")
          Future(MessageResponse(Some("not authorized"), code = HttpResponseStatus.UNAUTHORIZED, headers = Map(HttpHeaders.Names.WWW_AUTHENTICATE -> Seq("Basic realm=\"bad user or password\""))))
        }
      } else {
        logger.debug(s"bad auth header $header, method: ${http.method}, ip: ${http.remoteIp}")
        Future(MessageResponse(Some("not authorized"), code = HttpResponseStatus.UNAUTHORIZED, headers = Map(HttpHeaders.Names.WWW_AUTHENTICATE -> Seq("Basic realm=\"uptolike gateway\""))))
      }
    }
  }

  def authSync(http: MessageRequest, usersPass: Seq[String], process: MessageRequest => MessageResponse): MessageResponse = {
    val authHeaders = http.req.getHeaders(HttpHeaders.Names.AUTHORIZATION)

    if (authHeaders.isEmpty) {
      logger.debug(s"not authorized, method: ${http.method}, ip: ${http.remoteIp}")
      MessageResponse(Some("not authorized"), code = HttpResponseStatus.UNAUTHORIZED, headers = Map(HttpHeaders.Names.WWW_AUTHENTICATE -> Seq("Basic realm=\"uptolike gateway\"")))
    } else {
      val header = authHeaders.get(0)

      if (header.contains("Basic ")) {
        val auth = header.replace("Basic ", "")
        val userPass = Base64.decode(Unpooled.wrappedBuffer(auth.getBytes)).toString(Charset.forName("UTF8"))
        if (usersPass.contains(userPass)) {
          logger.debug(s"method: ${http.method}, user ${userPass.takeWhile(_ != ':')}, ip: ${http.remoteIp}")
          process(http)
        } else {
          logger.debug(s"method: ${http.method}, bad user password ${userPass.takeWhile(_ != ':')}, ip: ${http.remoteIp}")
          MessageResponse(Some("not authorized"), code = HttpResponseStatus.UNAUTHORIZED, headers = Map(HttpHeaders.Names.WWW_AUTHENTICATE -> Seq("Basic realm=\"bad user or password\"")))
        }
      } else {
        logger.debug(s"bad auth header $header, method: ${http.method}, ip: ${http.remoteIp}")
        MessageResponse(Some("not authorized"), code = HttpResponseStatus.UNAUTHORIZED, headers = Map(HttpHeaders.Names.WWW_AUTHENTICATE -> Seq("Basic realm=\"uptolike gateway\"")))
      }
    }
  }

}


object ContentType {
  val JS = "text/javascript;charset=UTF-8"
  val HTML = "text/html;charset=UTF-8"
  val JSON = "application/json;charset=UTF-8"
  val XML = "text/xml"
  val PLAIN = "text/plain"
  val GIF = "image/gif"
  val BINARY = "application/octet-stream"

  val crossDomain = "<?xml version=\"1.0\"?> <cross-domain-policy><allow-access-from domain=\"*\" to-ports=\"1200-5000\"/></cross-domain-policy>\u0000\u0000"
  val robots    = "User-agent: *\nDisallow: /"
}
