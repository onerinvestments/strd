package strd.net.http

import io.netty.handler.codec.http.HttpHeaders

/**
 * @author Kirill chEbba Chebunin
 */
object ContentType {
  val JS      = "application/javascript"
  val HTML    = "text/html;charset=UTF-8"
  val JSON    = "application/json;charset=UTF-8"
  val XML     = "text/xml"
  val PLAIN   = "text/plain"
  val GIF     = "image/gif"
  val BINARY  = "application/octet-stream"

  def apply(x: (this.type ) => String): (String, Seq[String]) = HttpHeaders.Names.CONTENT_TYPE -> Seq(x(this))
  def apply(s: String): (String, Seq[String]) = HttpHeaders.Names.CONTENT_TYPE -> Seq(s)
}
