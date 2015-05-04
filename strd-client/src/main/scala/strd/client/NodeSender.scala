//package strd.client
//
//import strd.cluster._
//import scala.concurrent.ExecutionContext
//import org.jboss.netty.channel.Channel
//import akka.actor.ActorRef
//import strd.core.StrdPackage
//import strd.tcp.ChannelWriter
//
///**
// * $Id$
// * $URL$
// * User: bulay
// * Date: 8/13/13
// * Time: 2:27 PM
// */
//class NodeSender(val clientDispatcher: ActorRef)(implicit val executionContext: ExecutionContext) extends NodeConnector[SenderData] {
//
//  def production = {
//    case Event(msg: MsgEvent, cd: SenderData) => {
//      msg.message match {
//        case p: PackageSaved =>
//          clientDispatcher ! p
//          stay()
//        case p: PackageFailed =>
//          clientDispatcher ! p
//          stay()
//        case p: PackageTimeout =>
//          clientDispatcher ! p
//          stay()
//        case p: ClientResponse =>
//          clientDispatcher ! p
//          stay()
//
//        case NodeShutdown =>
//          clientDispatcher ! NodeDisconnected(cd.meta.id)
//          goto(CLOSING) using EMPTY
//        case p:  NodeResponse =>
//          clientDispatcher ! p
//          stay()
//      }
//    }
//    case Event(pkg: StrdPackage, cd: SenderData) => {
//
//      ChannelWriter.writeToChannel(cd.ch, pkg)
//      stay()
//    }
//    case Event(req: StrdRequest[_], cd: SenderData) => {
//
//      ChannelWriter.writeToChannel(cd.ch, req)
//      stay()
//    }
//  }
//
//  def channelData(meta: DataNodeMeta, ch: Channel) = SenderData(meta, ch)
//
//  def onClose(cd: SenderData) {
//    logger.debug(s"node sender $cd <-- production")
//    clientDispatcher ! NodeDisconnected(cd.meta.id)
//  }
//
//  def onProductionStart(cd: SenderData) {
//    logger.debug(s"node sender $cd --> production")
//    clientDispatcher ! NodeConnected(cd.meta.id)
//  }
//
//  def onProductionEnd(cd: SenderData) {
//    logger.debug(s"node sender $cd <-- production")
//    clientDispatcher ! NodeDisconnected(cd.meta.id)
//  }
//
//  def processUnhandled(req: Any, state: DNConnectorState) {
//    req match {
//      case p: StrdPackage => state match {
//        case CLOSING => clientDispatcher ! NodeShutdown
//        case _ => clientDispatcher ! PackageTimeout(p.pkgId)
//      }
//      case _ => logger.error(s"unhandled $req")
//    }
//  }
//}
//
//case class SenderData(meta: DataNodeMeta,
//                      ch: Channel
//                       ) extends ChannelData {
//
//  def copyData(ch: Channel) = copy(ch = ch)
//
//}
//
//
//
//
