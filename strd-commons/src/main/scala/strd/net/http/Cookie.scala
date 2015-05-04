package strd.net.http

import io.netty.handler.codec.http._

import scala.collection.JavaConversions._

/**
 * @author Kirill chEbba Chebunin
 */
object Cookie {
  def apply(name: String, value: String, path: String = "/", domain: Option[String] = None, maxAge: Option[Long] = None): Cookie = {
    val cookie = new DefaultCookie(name, value)
    cookie.setPath(path)
    domain.foreach(cookie.setDomain)
    maxAge.foreach(cookie.setMaxAge)

    cookie
  }
  def getHeader(cookie: Cookie) = HttpHeaders.Names.SET_COOKIE -> Seq(ServerCookieEncoder.encode(cookie))
  def getHeader(cookies: Seq[Cookie]): (String, Seq[String]) = HttpHeaders.Names.SET_COOKIE -> ServerCookieEncoder.encode(cookies)
}
