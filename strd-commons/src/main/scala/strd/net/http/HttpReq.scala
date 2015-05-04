package strd.net.http

import java.nio.ByteBuffer
import java.nio.charset.Charset

import io.netty.handler.codec.http._
import strd.util.CTime

import scala.collection.convert.decorateAsScala._
import scala.util.Try

/**
 * @author Kirill chEbba Chebunin
 */
trait HttpReq {
  def id: Long
  def started: Long
  def method: HttpMethod
  def uri: String
  def headers: MultiStringMap
  def content: ByteBuffer
  def keepAlive: Boolean

  def ip: String

  lazy val (path, query) = {
    val decoder = new QueryStringDecoder(uri)
    (decoder.path().dropWhile(_ == '/'), MultiStringMap(decoder.parameters()))
  }

  lazy val cookies = {
    headers.get(HttpHeaders.Names.COOKIE).map { values =>
      values.flatMap { c =>
        Try(CookieDecoder.decode(c).asScala).getOrElse(Set.empty[Cookie])
      }.map { c =>
        c.getName -> c.getValue
      }.toMap
    }.getOrElse(Map.empty)
  }

  def processingTime = CTime.now - started


  lazy private val _params = new QueryStringDecoder(contentString, false).parameters()

  lazy val contentBytes =  {
    val b = new Array[Byte](content.limit())
    content.get(b)
    b
  }
  def contentString = new String(contentBytes, Charset.forName("UTF-8"))
  def contentParams = _params


  def host      : Option[String] = headers.getOne(HttpHeaders.Names.HOST)
  def userAgent : Option[String] = headers.getOne(HttpHeaders.Names.USER_AGENT)
  def referrer  : Option[String] = headers.getOne(HttpHeaders.Names.REFERER)


  override def toString =
    s"""
      |HttpReq: $id $started $ip
      |$method $uri
      |${headers.toSeq.flatMap(h => h._2.map(v => s"${h._1}: $v")).mkString("\n")}
      |
      |${content.limit()} bytes
    """.stripMargin.trim
}

