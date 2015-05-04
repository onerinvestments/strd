package strd.net.http

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util

import io.netty.buffer.Unpooled
import io.netty.channel.{Channel, ChannelHandlerContext}
import io.netty.handler.codec.http.HttpHeaders.Names._
import io.netty.handler.codec.http.HttpVersion._
import io.netty.handler.codec.http._
import io.netty.handler.codec.{MessageToMessageDecoder, MessageToMessageEncoder}
import strd.util.CTime

/**
 * @author Kirill chEbba Chebunin
 */
class StrdRequestDecoder extends MessageToMessageDecoder[FullHttpRequest]{
  var reqCounter: Long = 1L

  def getRemoteAddr(ch: Channel, headers: HttpHeaders) = {
    Option(headers.get("X-Real-Ip")).orElse(Option(headers.get("X-Forwarded-For")))
      .filterNot(ip => ip.contains(",") || ip == "unknown")
      .orElse(Option(ch.remoteAddress.asInstanceOf[InetSocketAddress].getAddress.getHostAddress))
      .getOrElse {
        throw new RuntimeException("can not fetch ip")
      }
  }

  override def decode(ctx: ChannelHandlerContext, msg: FullHttpRequest, out: util.List[AnyRef])  {
    val msgHeaders = msg.headers()
    val buf = msg.content()

    val req = new HttpReq {
      val id = reqCounter
      val started = CTime.now
      val method = msg.getMethod
      val uri = msg.getUri
      val headers = MultiStringMap(msgHeaders)
      val ip = getRemoteAddr(ctx.channel(), msgHeaders)
      val keepAlive = HttpHeaders.isKeepAlive(msg)

      val content = if (buf.readableBytes() > 0) buf.nioBuffer().asReadOnlyBuffer() else ByteBuffer.allocate(0)
    }

    out.add(req)
    reqCounter += 1
  }
}

class StrdResponseEncoder extends MessageToMessageEncoder[HttpResp] {

  def encode(ctx: ChannelHandlerContext, msg: HttpResp, out: util.List[AnyRef]) = {
    val response = new DefaultFullHttpResponse(
      HTTP_1_1,
      msg.status,
      msg.body.getOrElse(Unpooled.buffer(0))
    )

    val headers = response.headers()
    msg.headers.foreach {
      case (key, value:Seq[String]) =>
        value.foreach( v => headers.add(key, v))
    }

    // Set
    if (!headers.contains(CONTENT_TYPE) && msg.body.exists(_.readableBytes() > 0)) {
      headers.set(CONTENT_TYPE, ContentType.PLAIN)
    }

    // Set Content Length
    headers.set(CONTENT_LENGTH, response.content().readableBytes())

    // Set P3P for cookies
    if (headers.contains(SET_COOKIE)) {
      headers.set("P3P", "CP='IDC DSP COR ADM DEVi TAIi PSA PSD IVAi IVDi CONi HIS OUR IND CNT'")
    }

    out.add(response)
  }
}
