package utl.net.http

import java.net.InetSocketAddress
import java.util

import io.netty.buffer.ByteBuf
import io.netty.channel.{Channel, ChannelHandlerContext}
import io.netty.handler.codec.MessageToMessageDecoder
import io.netty.handler.codec.http._
import strd.util.{Stats2, CTime}

/**
 *
 * User: light
 * Date: 27/04/14
 * Time: 23:47
 */

class HttpCombiner extends MessageToMessageDecoder[HttpObject]{
  var reqCounter  : Long = 0L
  var invalid     : Boolean = false

  var method      : HttpMethod = null
  var host        : String = null
  var keepAlive   : Boolean = false
  var cookies     = Map.empty[String, String]
  var headers     = Map.empty[String, String]
  var path        : String = null
  var params      = Map.empty[String, String]
  var remoteIp    : String = null

  def cleanup() {
    method      = null
    host        = null
    path        = null
    remoteIp    = null
    keepAlive   = false
    params      = Map.empty[String, String]
    cookies     = Map.empty[String, String]
    headers     = Map.empty[String, String]
  }

  def appendCookies(headers: HttpHeaders) {
    val cookies = headers.getAll(HttpHeaders.Names.COOKIE)
    if (! cookies.isEmpty) {

      val iter = cookies.iterator()
      while (iter.hasNext) {
        val ck = iter.next()
        val cookies = CookieDecoder.decode(ck)

        val ckIter = cookies.iterator()
        while (ckIter.hasNext) {
          val cookie = ckIter.next()
          this.cookies = this.cookies + ( cookie.getName -> cookie.getValue )
        }
      }
    }
  }

  def appendParams(uri : String) {
    val decoder = new QueryStringDecoder(uri)
    this.path = decoder.path()

    val p = decoder.parameters()
    if (! p.isEmpty ) {
      val eset = p.entrySet()
      val iter = eset.iterator()
      while ( iter.hasNext) {
        val entry = iter.next()

        if (! entry.getValue.isEmpty)
          params = params + ( entry.getKey -> entry.getValue.get(0) )
      }
    }

  }

  def appendHeaders(headers: HttpHeaders) {
    val iter = headers.iterator()
    while (iter.hasNext) {
      val e = iter.next()
      this.headers += ( e.getKey -> e.getValue )
    }
  }

  def getRemoteAddr(ch: Channel, headers: HttpHeaders) = {
    Option(headers.get("X-Real-Ip"))
      .orElse(Option(ch.remoteAddress.asInstanceOf[InetSocketAddress].getAddress.getHostAddress))
      .getOrElse(throw new RuntimeException("can not fetch ip"))
  }

  override def decode(ctx: ChannelHandlerContext, msg: HttpObject, out: util.List[AnyRef])  {
    if (invalid)
      return

    msg match {
      case x: HttpRequest =>
        cleanup()

        try {
          val headers = x.headers()
          keepAlive = HttpHeaders.isKeepAlive(x)
          appendCookies(headers)
          appendHeaders(headers)
          appendParams(x.getUri)
          remoteIp = getRemoteAddr(ctx.channel(), headers)
          host = Option(HttpHeaders.getHost(x)).getOrElse(throw new RuntimeException("Unknown host"))
          method = x.getMethod

          if (x.headers().contains("Expect")) {
            Stats2.incr("HTTP_EXPECT_FAILED")
            invalid = true
            ctx.close()
          }

        } catch {
          case x : Exception =>
            x.printStackTrace()
            invalid = true
            ctx.close()
        }



      case x: HttpContent =>
        out.add( HttpReq(
          httpMethod= method,
          host      = host,
          path      = path,
          params    = params,
          remoteIp  = remoteIp,
          cookies   = cookies,
          reqId     = reqCounter,
          keepAlive = keepAlive,
          started   = CTime.now,
          body     = {
            val buf = x.content()
            val b = new Array[Byte](buf.readableBytes())
            buf.getBytes(buf.readerIndex, b)
            b
          },
          headers   = headers
        ))

        reqCounter += 1

    }
  }
}

case class HttpReq( httpMethod: HttpMethod,
                    host      : String,
                    path      : String,
                    params    : Map[String, String],
                    remoteIp  : String,
                    cookies   : Map[String, String],
                    reqId     : Long,
                    keepAlive : Boolean,
                    started   : Long,
                    body      : Array[Byte],
                    headers   : Map[String,String]) {

  def processingTime = (CTime.now - started).toInt

  def content = new String(body, "UTF-8")

  def userAgent = headers.getOrElse(HttpHeaders.Names.USER_AGENT, "")

  def referrer = headers.get(HttpHeaders.Names.REFERER)

  def parameter(name: String) = params.getOrElse(name, throw new BadRequest(HttpResponseStatus.BAD_REQUEST, Some(s"parameter $name is required")))

  def isHTTPS = headers.get("X_FORWARDED_PROTO").exists(_.equals("https"))

  def proto = if (isHTTPS) "https://" else "http://"

  def baseUrl = s"$proto$host"

  @deprecated("Saved for legacy purposes. Use 'path' for routing", "Netty 4 HttpReq")
  lazy val method = {
    val idx = path.lastIndexOf("/")
    if (idx < 0) throw new IllegalArgumentException("empty method name")

    path.substring(idx + 1)
  }
}

case class HttpResp(status      : HttpResponseStatus = HttpResponseStatus.OK,
                    contentType : Option[String] = None,
                    body        : Option[ByteBuf] = None,
                    headers     : Map[String, Seq[String]] = Map.empty)

case class BadRequest(status: HttpResponseStatus, msg: Option[String] = None) extends Exception
