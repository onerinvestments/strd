package strd.client

import org.jboss.netty.channel.{MessageEvent, ExceptionEvent, ChannelHandlerContext, SimpleChannelHandler}
import java.nio.channels.ClosedChannelException
import org.slf4j.LoggerFactory
import akka.actor.ActorRef
import akka.pattern.ask
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors
import scala.util.{Failure, Success}
import akka.util.Timeout
import concurrent.duration._
import strd.trace.PoolContext

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 9/10/13
 * Time: 2:23 PM
 */
class RemoteHandler(val actor: ActorRef, requestTimeout: FiniteDuration = 10.minutes) extends SimpleChannelHandler{

  val log = LoggerFactory.getLogger(getClass)
  implicit val executionContext = PoolContext.cachedExecutor()
  implicit val timeout = Timeout(requestTimeout)

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val future = actor ? e.getMessage
    future.onComplete{
      case Success(res) => ctx.getChannel.write(res)
      case Failure(t) => ctx.getChannel.write(t)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    e.getCause match {
      case e: ClosedChannelException => log.warn(s"channel closed ${ctx.getChannel.getRemoteAddress}")
      case t: Exception => log.error("netty error", e.getCause)
    }
  }

}
