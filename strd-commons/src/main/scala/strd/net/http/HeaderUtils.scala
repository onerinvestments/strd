package strd.net.http

import io.netty.handler.codec.http._

/**
 * @author Kirill chEbba Chebunin
 */
trait HeaderUtils {
  val NoCache = HttpHeaders.Names.CACHE_CONTROL -> Seq("no-cache,no-store,max-age=0,must-revalidate")

  val KeepAlive = HttpHeaders.Names.CONNECTION -> Seq(HttpHeaders.Values.KEEP_ALIVE)
  val Close = HttpHeaders.Names.CONNECTION -> Seq(HttpHeaders.Values.CLOSE)

  def Cookies(cookies: Cookie*): MultiString = HttpHeaders.Names.SET_COOKIE -> cookies.map(ServerCookieEncoder.encode)
}
