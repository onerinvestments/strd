package strd.net.http

import java.util.concurrent._
import java.util.concurrent.atomic._

import io.netty.channel.{ChannelFuture, ChannelFutureListener, ChannelHandlerContext}
import io.netty.handler.codec.http.HttpResponseStatus
import org.slf4j.LoggerFactory
import strd.util.CTime

import scala.annotation.tailrec

trait HttpStream {
  def startRequest(req: HttpReq)
  def sendResponse(reqId: Long, resp: HttpResp)
  def stop()
}

object HttpStream {
  def factory(scheduler: ScheduledExecutorService, timeout: Int) = (ctx: ChannelHandlerContext, req: HttpReq) => {
    if (req.keepAlive) {
      new QueueHttpStream(ctx, timeout, scheduler)
    } else {
      new SingleHttpStream(ctx, timeout, scheduler)
    }
  }
}

class SingleHttpStream(ctx: ChannelHandlerContext, timeout: Int, scheduler: ScheduledExecutorService) extends HttpStream {
  val log = LoggerFactory.getLogger(getClass)

  val deadliner = new Runnable {
    def run() {
      if (scheduledWrite.compareAndSet(false, true)) {
        HttpStats.deadline()
        writeResponse(HttpResp(HttpResponseStatus.SERVICE_UNAVAILABLE))
      }
    }
  }
  
  var request: Option[HttpReq] = None
  val scheduledWrite = new AtomicBoolean(false)

  def startRequest(req: HttpReq) {
    request.fold {
      request = Some(req)
      scheduler.schedule(deadliner, timeout, TimeUnit.MILLISECONDS)
    } { old =>
      throw new IllegalStateException(s"Can not start ${req.id}. Single request ${old.id} was already started")
    }
  }

  def sendResponse(reqId: Long, resp: HttpResp) {
    request match {
      case Some(r) =>
        if (r.id != reqId || !scheduledWrite.compareAndSet(false, true)) {
          HttpStats.unorderedReq()
        } else {
          HttpStats.time(CTime.now - r.started)
          writeResponse(resp)
        }
      case None =>
        HttpStats.unorderedReq(empty = true)
    }
  }

  def clear() {
    request = None
  }

  def stop() {
    clear()
  }

  def writeResponse(resp: HttpResp) {
    HttpStats.response(resp)
    ctx.write(resp + Close)
    clear()
    ctx.flush()
    ctx.close()
  }
}

class QueueHttpStream(ctx: ChannelHandlerContext, timeout: Int, scheduler: ScheduledExecutorService) extends HttpStream {
  val counter = new AtomicInteger()
  val streamStarted = CTime.now

  lazy val log              = LoggerFactory.getLogger(getClass)

  private val requests      = new PriorityBlockingQueue[StreamReqId](1024)
  private val responses     = new PriorityBlockingQueue[StreamResponse](1024)

  private val scheduledWrite = new AtomicBoolean()

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

  def startRequest(req: HttpReq) {
    counter.incrementAndGet()

    val scheduled = scheduler.schedule( new Runnable {
      override def run() = {
        ctx.executor().execute(flushQueue)
      }

    }, timeout, TimeUnit.MILLISECONDS)

    requests.offer(new StreamReqId(req.id, req.started, scheduled))
  }

  def sendResponse(reqId: Long, resp: HttpResp) {
    val firstReq = requests.peek()

    if (firstReq == null) {
      HttpStats.unorderedReq(empty = true)
    } else if (firstReq.id > reqId) {
      HttpStats.unorderedReq()
      //  illegal state exception -> skip response
      log.warn("Drop unordered response")
    } else {
      if (!responses.offer(StreamResponse(reqId, resp), 3, TimeUnit.SECONDS)) {
        HttpStats.queueTimeout()
        log.warn("Can not submit response")
      } else if (reqId == firstReq.id) {
        schedulePurge()
      }
    }
  }

  def stop() {
    val now = CTime.now

    HttpStats.keepAlive(counter.get, now - streamStarted)

    this.responses.clear()
    this.requests.clear()
  }

  def schedulePurge() {
    if (scheduledWrite.compareAndSet(false, true)) {
      ctx.executor().execute(flushQueue)
    }
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

      HttpStats.time(CTime.now - req.ts)
      writeResponse(resp.get.resp)

      if (reqIter.hasNext && respIter.hasNext ) {
        purge0(reqIter, respIter, needFlush = true)
      } else {
        true
      }
    } else {
      needFlush
    }
  }

  @tailrec
  final def purgeReqIter( req : java.util.Iterator[StreamReqId]) : Option[StreamReqId] = {
    val time = CTime.now
    val r = req.next()
    if (r.ts + timeout < time) {
      HttpStats.deadline()
      req.remove()
      writeDeadLineResponse(ctx)

      if (req.hasNext)
        purgeReqIter( req)
      else
        None
    } else Some(r)

  }

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

  def writeResponse(resp: HttpResp) {
    HttpStats.response(resp)
    ctx.write(resp + KeepAlive)
  }

  def writeDeadLineResponse(ctx : ChannelHandlerContext) {
    writeResponse(HttpResp(HttpResponseStatus.SERVICE_UNAVAILABLE))
    ctx.flush()
  }
}

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
