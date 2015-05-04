package strd.net.http

import io.netty.buffer.ByteBuf
import io.netty.handler.codec.http.HttpHeaders.Names._
import io.netty.handler.codec.http.HttpResponseStatus
import io.netty.handler.codec.http.HttpResponseStatus._

/**
 * @author Kirill chEbba Chebunin
 */
case class HttpResp(status      : HttpResponseStatus = OK,
                    body        : Option[ByteBuf] = None,
                    headers     : MultiStringMap = Map.empty) {

  def +(kv: (String, Seq[String])) = copy(headers = headers + kv)
  def :+(kv: (String, String)) = copy(headers = headers :+ kv)
}

object HttpResp {
  val EMPTY       = HttpResp(NO_CONTENT)
  val BAD_REQ     = HttpResp(BAD_REQUEST)

  def pixel(headers: Map[String, Seq[String]] = Map.empty) = HttpResp(
    headers = headers + ContentType(_.GIF),
    body    = Some(Content(EMPTY_GIF))
  )

  def redirect(url: String, headers: Map[String, Seq[String]] = Map.empty) = HttpResp(
    status = FOUND,
    headers = headers :+ (LOCATION -> url)
  )
}
