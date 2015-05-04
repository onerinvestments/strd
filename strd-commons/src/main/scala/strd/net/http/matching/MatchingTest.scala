package strd.net.http.matching

import java.nio.ByteBuffer

import io.netty.handler.codec.http.HttpMethod
import strd.net.http._
import strd.util._

/**
 * @author Kirill chEbba Chebunin
 */
object MatchingTest extends App {
  val req = new HttpReq {
    val ip: String = ""

    val uri: String = "foo/bar/14/baz?param1=value1&param2=value2"

    val method: HttpMethod = HttpMethod.POST

    val keepAlive: Boolean = true

    val content: ByteBuffer = ByteBuffer.allocate(0)

    val headers: MultiStringMap = Map.empty

    val started: Long = CTime.now

    val id: Long = 1
  }

  req match {
    case HttpMethod.POST -> "foo/bar/14/baz" =>
      println("matched")
    case _ =>
      println("Not matched")
  }

  req match {
    case HttpMethod.POST -> "foo" / "bar" / "14" / "baz" =>
      println(s"matched")
    case _ =>
      println("Not matched")
  }

  req match {
    case HttpMethod.POST -> "foo" / x / "14" / y =>
      println(s"matched $x, $y")
    case _ =>
      println("Not matched")
  }

  req match {
    case HttpMethod.POST -> "foo/bar" / "14" / x =>
      println(s"matched $x")
    case _ =>
      println("Not matched")
  }

  req match {
    case HttpMethod.POST -> "foo" /: x =>
      println(s"matched $x")
    case _ =>
      println("Not matched")
  }

  req match {
    case HttpMethod.POST -> "foo" /: "bar" /: x =>
      println(s"matched $x")
    case _ =>
      println("Not matched")
  }

  req match {
    case HttpMethod.POST -> "foo" /: "bar" /: x =>
      println(s"matched $x")
    case _ =>
      println("Not matched")
  }

  req match {
    case HttpMethod.POST -> r"""foo/(.*)$x/(\d+)$y/baz""" =>
      println(s"matched $x, $y")
    case _ =>
      println("Not matched")
  }

  val Param1 = new ScalarParam[String]("param1")
  val Param2 = new ScalarParam[String]("param2")

  req match {
    case HttpMethod.POST -> "foo" /: "bar" /: x :? Param1(y) =>
      println(s"matched $x, $y")
    case _ =>
      println("Not matched")
  }

  req match {
    case x -> "foo" / "bar" / "14" / "baz" :? Param1(y) & Param2(z) =>
      println(s"matched $x, $y, $z")
    case _ =>
      println("Not matched")
  }

//  req match {
//    case x -> "foo" / "bar" / "14" / "baz" :? p.param1.str(y) & p.param2.str(z) =>
//      println(s"matched $x, $z")
//    case _ =>
//      println("Not matched")
//  }
}
