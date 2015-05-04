package strd.client

import org.jboss.netty.channel._
import akka.actor.{ActorRef, Cancellable, FSM, Actor}
import org.jboss.netty.bootstrap.ClientBootstrap
import strd.tcp.{ChannelWriter, TcpPipeline}
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import concurrent.duration._
import scala.concurrent.ExecutionContext
import strd.cluster._
import strd.cluster.NodeDisconnected

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 8/13/13
 * Time: 2:42 PM */
class NodeConnector(val clientDispatcher: ActorRef, val conName : String)(implicit val executionContext: ExecutionContext) extends SimpleChannelHandler with Actor with FSM[DNConnectorState, DNConnectorData] {

  val bootstrap = new ClientBootstrap(TcpClientFactory)
  bootstrap.setPipelineFactory(TcpPipeline.build(this, conName, 2L * 1024 * 1024 * 1024))
  val logger = LoggerFactory.getLogger(getClass)

  startWith(INIT, EMPTY)

  when(INIT) {
    case Event(c: ConnectReq, EMPTY) => {
      logger.debug(s"start connect to node $c")
      val channelFuture = bootstrap.connect(new InetSocketAddress(c.host, c.port))
      context.system.scheduler.scheduleOnce(30.seconds, self, TryReconnect)
      goto(CONNECTION) using ChannelData(c, channelFuture.getChannel)
    }
  }

  when(CONNECTION) {
    case Event(c: ConnectedEvent, cd: ChannelData) => {
      logger.debug(s"node ${cd.meta} connected ")
      goto(PRODUCTION) using cd.copy(ch = c.ch)
    }
    case Event(TryReconnect, cd: ChannelData) => {
      val schedule = context.system.scheduler.schedule(5.second, 5.second, self, TryReconnect)
      goto(AWAIT_RECONNECT) using ReconnectionData(schedule, cd)
    }
  }

  when(PRODUCTION) {
    case Event(msg: MsgEvent, cd: ChannelData) => {
      clientDispatcher ! msg.message
      stay()
    }
    case Event(r: ClusterRequest, cd: ChannelData) => {
      ChannelWriter.writeToChannel(cd.ch, r)
      stay()
    }
    case Event(TryReconnect, cd: ChannelData) => {
      // skip
      stay()
    }

  }

  when(AWAIT_RECONNECT) {
    case Event(TryReconnect, rd: ReconnectionData) => {

      reconnectAttempt(rd)
    }
    case Event(e: ErrorEvent, rd: ReconnectionData) => {
      logger.debug(s"node connection ${rd.cd.meta} error ", e.t)
      //reconnectAttempt(rd)
      stay()
    }

    case Event(ce: ConnectedEvent, rd: ReconnectionData) => {
      logger.debug(s"node ${rd.cd.meta} reconnected ")
      goto(PRODUCTION) using rd.cd.copy(ch = ce.ch)
    }

    case Event(e: DisconnectedEvent, rd: ReconnectionData) => {
      logger.debug(s"node ${rd.cd.meta} disconnected ")
      stay()
    }

  }


  def reconnectAttempt(rd: ReconnectionData) = {
    val channelFuture = bootstrap.connect(new InetSocketAddress(rd.cd.meta.host, rd.cd.meta.port))
    stay() using rd.copy(attemptReconnect = math.max(rd.attemptReconnect - 1, 0), cd = rd.cd.copy(ch = channelFuture.getChannel))
  }

  when(CLOSING) {
    case Event(_, _) => {
      stay()
    }
  }

  whenUnhandled {
    case Event(e: ErrorEvent, cd: ChannelData) => {
      logger.debug(s"node connection ${cd.meta} error ${e.t}")
      val schedule = context.system.scheduler.schedule(5.second, 5.second, self, TryReconnect)
      goto(AWAIT_RECONNECT) using ReconnectionData(schedule, cd)
    }
    case Event(e: DisconnectedEvent, cd: ChannelData) => {
      logger.debug(s"node ${cd.meta} disconnected ")
      val schedule = context.system.scheduler.schedule(1.second, 5.second, self, TryReconnect)
      goto(AWAIT_RECONNECT) using ReconnectionData(schedule, cd)
    }
    case Event(CloseConnection, _) => {
      goto(CLOSING)
    }
    case Event(r: ClusterRequest, _) => {
      clientDispatcher ! RequestFailed(r.reqId, FailReason.Unknown, Some(s"bad connector state $stateName"))
      stay()
    }
    case Event(event, data) => {
      logger.error(s"UNHANDLED \n\tSTATE = $stateName \n\tEVENT = $event \n\tDATA $data")
      //processUnhandled(event, stateName)
      stay()
    }
  }

  onTransition {
    case _ -> PRODUCTION =>
      stateData match {
        case rd: ReconnectionData => rd.cancellable.cancel()
        case _ =>
      }
      nextStateData match {
        case cd: ChannelData =>
          logger.debug(s"go to production $cd")
          clientDispatcher ! NodeConnected(cd.meta.id)
      }

    case _ -> AWAIT_RECONNECT =>
      stateData match {
        case cd: ChannelData => clientDispatcher ! NodeDisconnected(cd.meta.id)
        case _ =>
      }

    case _ -> CLOSING => {
      stateData match {
        case cd: ChannelData =>
          logger.debug(s"node base connector ${cd.meta} stopped")
          clientDispatcher ! NodeConnClosed(cd.meta.id)
          cd.ch.close()
        case rd: ReconnectionData =>
          logger.debug(s"node connector reconnect ${rd.cd.meta} stopped")
          clientDispatcher ! NodeConnClosed(rd.cd.meta.id)
          rd.cancellable.cancel()
          rd.cd.ch.close()
      }
      stop()
    }
  }


  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    self ! MsgEvent(e.getMessage, ctx.getChannel)
  }

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    self ! ConnectedEvent(ctx.getChannel)
  }

  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    self ! DisconnectedEvent(ctx.getChannel)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    self ! ErrorEvent(ctx.getChannel, e.getCause)
  }
}

trait DNConnectorState

trait DNConnectorData

object INIT extends DNConnectorState

object CONNECTION extends DNConnectorState

object PRODUCTION extends DNConnectorState

object AWAIT_RECONNECT extends DNConnectorState

object CLOSING extends DNConnectorState


object EMPTY extends DNConnectorData

case class MsgEvent(message: Any, ch: Channel)

case class ConnectedEvent(ch: Channel)

case class DisconnectedEvent(ch: Channel)

case class ErrorEvent(ch: Channel, t: Throwable)

case class ConnectReq(id: Int, host: String, port: Int)

case object TryReconnect

case class ReconnectionData(cancellable: Cancellable, cd: ChannelData, attemptReconnect: Int = 3) extends DNConnectorData

//case class CloseData(promise: Promise[Boolean]) extends DNConnectorData

case class ChannelData(meta: ConnectReq, ch: Channel) extends DNConnectorData