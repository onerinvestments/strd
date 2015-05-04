package strd.net.http

import java.util.concurrent.atomic.AtomicInteger

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.handler.codec.http.HttpResponseStatus
import org.slf4j.LoggerFactory
import strd.util.{TimerCounter, TimerStats}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait ChannelHandler extends SimpleChannelInboundHandler[HttpReq] {
  lazy val log = LoggerFactory.getLogger(getClass)

  ChannelHandler.channels.incrementAndGet()

  def handle(ctx: ChannelHandlerContext, req: HttpReq)

  def channelRead0(ctx: ChannelHandlerContext, req: HttpReq) {
    ChannelHandler.rps.increment()
    HttpStats.request()
    handle(ctx, req)
  }

  override def channelUnregistered(ctx: ChannelHandlerContext) = {
    super.channelUnregistered(ctx)
    ChannelHandler.channels.decrementAndGet()
  }

  override def channelReadComplete(ctx: ChannelHandlerContext) = {
    super.channelReadComplete(ctx)
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
    super.channelReadComplete(ctx)
    log.warn("Http Channel error", cause)
    ctx.close()
  }
}


object ChannelHandler {
  val log = LoggerFactory.getLogger(getClass)

  val rps = new TimerStats {
    override def onTimer(counter: TimerCounter, n: Long) {
      if (n % 20 == 0) {
        log.debug("HttpServer RPS: " + counter.perSecond)
      }
    }
  }

  val channels = new AtomicInteger()

  HttpStats.channels { channels.get() }
  HttpStats.rps { rps.get() }
}

class DefaultChannelHandler(handler: RequestHandler, queryFactory: (ChannelHandlerContext, HttpReq) => HttpStream)
                           (implicit execctx: ExecutionContext) extends ChannelHandler {

  private var stream: Option[HttpStream] = None

  private def getStream(ctx: ChannelHandlerContext, req: HttpReq) = {
    stream.getOrElse {
      val q = queryFactory(ctx, req)
      stream = Some(q)
      q
    }
  }

  def handle(ctx: ChannelHandlerContext, req: HttpReq) {
    val stream = getStream(ctx, req)

    stream.startRequest(req)

    handler.handle(req).onComplete {
      case Success(resp) =>
        HttpStats.handler(success = true)
        stream.sendResponse(req.id, resp)
      case Failure(e) =>
        HttpStats.handler(success = false)
        log.warn("Unexpected Http error", e)
        stream.sendResponse(req.id, HttpResp(status = HttpResponseStatus.INTERNAL_SERVER_ERROR))
    }
  }

  override def channelInactive(ctx: ChannelHandlerContext) {
    super.channelInactive(ctx)
    stream.foreach(_.stop())
  }
}
