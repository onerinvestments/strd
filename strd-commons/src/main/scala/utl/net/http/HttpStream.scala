package utl.net.http

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.{Date, Locale}

import com.twitter.ostrich.stats.Stats
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelFuture, ChannelFutureListener, ChannelHandlerContext}
import io.netty.handler.codec.http.HttpHeaders.Names._
import io.netty.handler.codec.http.HttpHeaders.Values
import io.netty.handler.codec.http.HttpResponseStatus._
import io.netty.handler.codec.http.HttpVersion._
import io.netty.handler.codec.http.{DefaultFullHttpResponse, HttpResponseStatus}
import io.netty.util.CharsetUtil
import org.apache.commons.lang.time.{DateFormatUtils, DateUtils}
import org.slf4j.LoggerFactory
import strd.util.CTime
import utl.core.http.ContentType

import scala.annotation.tailrec

/**
 *
 * User: light
 * Date: 28/04/14
 * Time: 00:29
 */

class StreamReqId(val id : Long, val ts : Long, val cancelFuture : ScheduledFuture[_]) extends Comparable[StreamReqId] {

  override def equals(other: Any): Boolean = other match {
    case that: StreamReqId => id == that.id
    case _ => false
  }


  override def compareTo(o: StreamReqId) = {
    if (o.id > id) {
      1
    } else if (o.id < id) {
      -1
    } else {
      0
    }
  }

  override def hashCode(): Int = {
    (id * 31).toInt
  }
}

case class StreamResponse(reqId: Long, resp: HttpResp) extends Comparable[StreamResponse] {
  override def compareTo(o: StreamResponse) = {
    (reqId - o.reqId).toInt
  }
}

object HttpHelper {
  //val EMPTY_BUF       = Unpooled.buffer(0)

  val emptyGif = {
    val stream = Thread.currentThread().getContextClassLoader.getResourceAsStream("empty.gif")
    if (stream == null ) {
      throw new RuntimeException("Can not find resource :empty.gif")
    }
    val ba = new Array[Byte](stream.available())
    stream.read(ba)
    ba
  }

  def pixel(headers: Map[String, Seq[String]] = Map.empty) = HttpResp(
    contentType = Some(ContentType.GIF),
    headers = headers,
    body = Some(Unpooled.wrappedBuffer(emptyGif))
  )

  def redirect(url: String, headers: Map[String, Seq[String]] = Map.empty) = HttpResp(
    status = HttpResponseStatus.FOUND,
    headers = (headers.toSeq :+ (LOCATION -> Seq(url))).toMap
  )

  def noContent( headers: Map[String, Seq[String]] = Map.empty) = HttpResp(
    status = HttpResponseStatus.NO_CONTENT,
    headers = headers
  )


  def writeResponse(ctx : ChannelHandlerContext, resp : HttpResp, keepAlive: Boolean = false) {
    import scala.collection.convert.decorateAsJava._

    val response = new DefaultFullHttpResponse(HTTP_1_1, resp.status, resp.body.getOrElse(Unpooled.buffer(0)))

    val headers = response.headers()
    resp.headers.foreach( x => headers.set(x._1, x._2.asJava))
    resp.contentType.map( x => headers.set(CONTENT_TYPE, x))
    if (resp.headers.contains(SET_COOKIE)) {
      headers.set("P3P", "CP='IDC DSP COR ADM DEVi TAIi PSA PSD IVAi IVDi CONi HIS OUR IND CNT'")
    }

    headers.set(CONTENT_LENGTH, response.content().readableBytes())

    /////
    headers.set(CACHE_CONTROL, Values.NO_STORE)
    headers.set(PRAGMA, Values.NO_CACHE)
    headers.set(EXPIRES, DateFormatUtils.formatUTC(DateUtils.addSeconds(new Date, 0), "EEE, dd MMM yyyy kk:mm:ss z", Locale.US))
    headers.set(DATE, DateFormatUtils.formatUTC(new Date, "EEE, dd MMM yyyy kk:mm:ss z", Locale.US))

    ////

    if (keepAlive) {
      // Add keep alive header as per:
      // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
      headers.set(CONNECTION, Values.KEEP_ALIVE)
      ctx.write(response)
    } else {
      headers.set(CONNECTION, Values.CLOSE)
      ctx.write(response)
//        .addListener(ChannelFutureListener.CLOSE)
    }

//    ctx.flush()
  }

  val log = LoggerFactory.getLogger(getClass)

  def responseToBuf( str : String ): ByteBuf = {
    Unpooled.wrappedBuffer( str.getBytes(CharsetUtil.UTF_8) )
  }

  def responseToBuf( bytes : Array[Byte] ): ByteBuf = {
    Unpooled.wrappedBuffer( bytes )
  }

  def safeBadRequest(error: String, trace : String): HttpResp = {
    log.warn(s"bad request ($error): $trace")
    Stats.incr("error/rtb/"+error)
    HttpResp(NO_CONTENT)
  }

  def badRequest(error: String, trace : String): HttpResp = {
    log.warn(s"bad request ($error): $trace")
    Stats.incr("error/rtb/"+error)
    HttpResp(BAD_REQUEST, body = Some( responseToBuf( error)) )
  }
}

class HttpStream(val ctx : ChannelHandlerContext,
                 timeout : Int,
                 sched   : ScheduledExecutorService) {

  val counter = new AtomicInteger()
  val streamStarted = CTime.now

  private val scheduledWrite = new AtomicBoolean()

  def startRequest(id : Long, time : Long) {
    counter.incrementAndGet()

    val scheduled = sched.schedule( new Runnable {
      override def run() = {
        ctx.executor().execute(flushQueue)
      }

    }, timeout, TimeUnit.MILLISECONDS)

    requests.offer( new StreamReqId(id, time, scheduled) )
  }


  def schedulePurge() {
    if (scheduledWrite.compareAndSet(false, true)) {
      ctx.executor().execute(flushQueue)
    }
  }

  lazy val log              = LoggerFactory.getLogger(getClass)

  private val requests      = new PriorityBlockingQueue[StreamReqId](1024)
  private val responses     = new PriorityBlockingQueue[StreamResponse](1024)


  @tailrec
  final def purgeRespIter(reqId : Long, resp : java.util.Iterator[StreamResponse]) : Option[StreamResponse] = {
    if (!resp.hasNext) return None

    val r = resp.next()
    if (r.reqId < reqId) {
      resp.remove()
      if (resp.hasNext)
        purgeRespIter(reqId, resp)
      else
        None
    } else Some(r)

  }

  @tailrec
  final def purgeReqIter( req : java.util.Iterator[StreamReqId]) : Option[StreamReqId] = {
    val time = CTime.now
    val r = req.next()
    if (r.ts + timeout < time) {
      Stats.incr("http/deadline")
      req.remove()
      writeDeadLineResponse(ctx)

      if (req.hasNext)
        purgeReqIter( req)
      else
        None
    } else Some(r)

  }

  @tailrec
  final def purge0(reqIter :  java.util.Iterator[StreamReqId], respIter : java.util.Iterator[StreamResponse], needFlush : Boolean ) : Boolean = {
    val reqOpt = if (reqIter.hasNext) purgeReqIter(reqIter) else None

    val resp = purgeRespIter(reqOpt.map(_.id).getOrElse(Long.MaxValue), respIter)

    if (reqOpt.isEmpty) {
      return needFlush
    }

    val req = reqOpt.get

    if (resp.isDefined && resp.get.reqId == req.id) {
      reqIter.remove()
      respIter.remove()

      try {
        req.cancelFuture.cancel(false)
      } catch {
        case ignored: Exception =>
      }

      HttpHelper.writeResponse(ctx, resp.get.resp, keepAlive = true)
      Stats.addMetric("http/ok", (CTime.now - req.ts).toInt)

      if (reqIter.hasNext  && respIter.hasNext )
        purge0(reqIter, respIter, needFlush = true)
      else {
        true
      }
    } else {
      needFlush
    }
  }

  val flushQueue = new Runnable {
    override def run() = {
      scheduledWrite.set(false)
      try {

        val reqIter = requests.iterator()
        val respIter = responses.iterator()

        val needFlush = purge0(reqIter, respIter, needFlush = false)
        if (needFlush) {
          ctx.flush()
        }

      } catch {
        case x: Exception => log.error("Failed", x)
      }
    }
  }

  val channelWriteListener = new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture) = {
        if ( future.cause() != null ) {
          log.error("Unexpected write exception", future.cause())
        }
      }
    }

  def appendResponse(resp : StreamResponse) {
    val firstReq = requests.peek()
    if (firstReq == null) {
      Stats.incr("http/unordered/dropOnAppend1")
      return
    }

    if (firstReq.id > resp.reqId) {
      Stats.incr("http/unordered/dropOnAppend2")
      //  illegal state exception -> skip response
      log.warn("Drop unordered response")
    } else {
      if( ! responses.offer( resp, 3, TimeUnit.SECONDS )) {
        Stats.incr("http/submit_resp/timeout")
        log.warn("Can not submit response")
        return
      }

      if (resp.reqId == firstReq.id) {
        schedulePurge()
      }
    }
  }


  def writeDeadLineResponse(ctx : ChannelHandlerContext) {
    HttpHelper.writeResponse(ctx,  HttpResp(HttpResponseStatus.NO_CONTENT), keepAlive = true)
    ctx.flush()
  }



  def stop() {
    val now = CTime.now

    Stats.addMetric("http/keep_alive_ms", (now - streamStarted).toInt)
    Stats.addMetric("http/keep_alive_count", counter.get)

    this.responses.clear()
    this.requests.clear()
  }

/*
  @tailrec
  private def purge0() { // working in connection thread

    val currentTime = CTime.now

    val firstReq  = requests.peek()
    if (firstReq != null) {

      if (firstReq.ts + timeout - 6 <= currentTime) {
        Stats.incr("http/timeout/purge0")
        log.warn("Deadline on request: " + firstReq.id + " timeout: " + timeout)
        requests.remove(firstReq)

        writeDeadLineResponse(ctx, firstReq.id)

        purge0()
      } else {
        val firstResp = responses.poll(0, TimeUnit.MILLISECONDS)

        if (firstResp != null) {

          if (firstResp.reqId == firstReq.id) {

            requests.remove(firstReq)
            HttpHelper.writeResponse(ctx, firstResp)

            Stats.addMetric("http/ok", (CTime.now - firstReq.ts).toInt)

            purge0()
          } else if (firstResp.reqId < firstReq.id) {
            Stats.incr("http/unordered/purge0")
            //  illegal state exception -> skip response
            log.warn("Drop unordered response: " + firstResp.reqId)
          }
        }
      }
    }
  }

*/

}
