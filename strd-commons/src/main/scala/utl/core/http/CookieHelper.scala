package utl.core.http

import io.netty.handler.codec.http._

import scala.collection.JavaConversions._

object CookieHelper {
  def apply(name: String, value: String, domain: Option[String] = None, maxAge: Option[Long] = None): Cookie = {
    val cookie = new DefaultCookie(name, value)
    cookie.setPath("/")
    domain.foreach(cookie.setDomain)
    maxAge.foreach(cookie.setMaxAge)

    cookie
  }
  def getHeader(cookie: Cookie) = HttpHeaders.Names.SET_COOKIE -> Seq(ServerCookieEncoder.encode(cookie))
  def getHeader(cookies: Seq[Cookie]): (String, Seq[String]) = HttpHeaders.Names.SET_COOKIE -> ServerCookieEncoder.encode(cookies)

  def addCookie(map: Map[String, Seq[String]], cookie: Cookie) = {
    val newValue = map.getOrElse(HttpHeaders.Names.SET_COOKIE, Seq.empty[String]) :+ ServerCookieEncoder.encode(cookie)
    map + (HttpHeaders.Names.SET_COOKIE -> newValue)
  }
}


