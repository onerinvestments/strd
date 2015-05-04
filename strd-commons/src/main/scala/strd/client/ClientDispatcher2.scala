package strd.client

import akka.actor.{OneForOneStrategy, _}
import com.twitter.ostrich.stats.Stats
import org.slf4j.LoggerFactory
import strd.cluster.FailReason._
import strd.cluster.{DataNodeMeta, NodeConnClosed, NodeConnected, RequestFailed, _}

import scala.collection.mutable
import scala.compat.Platform
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 8/19/13
 * Time: 2:54 PM
 */
class ClientDispatcher2(implicit val executionContext: ExecutionContext) extends Actor {

  val log = LoggerFactory.getLogger(getClass)
  val requests = mutable.Map.empty[Long, RequestWithTimeout2]
  val timeout = 10.seconds.toMillis
  var nodes = mutable.Map.empty[Int, NodeStatus]
  var requestsCount = 0
  var shutdownSender: Option[ActorRef] = None

  private val timeoutTask = context.system.scheduler.schedule(30.seconds, 5.second, self, CheckTimeout)


  def receive = {

    case GetQueueSize => sender ! requestsCount

    case GetActiveNodes => sender ! nodes.values.filter(_.started).toSeq

    case node: NodeStatus => {
      nodes += node.nodeId -> node
      node.actor ! ConnectReq(node.getMeta.id, node.getMeta.host, node.getMeta.port)
    }

    case meta: NodeMeta => {
      nodes.get(meta.id).map(_.setMeta( meta)).getOrElse(log.warn(s"no connector for $meta"))
    }

    case r: RemoveNode => {
      nodes.get(r.nodeId).map(closeNodeConn(_, r.timeout))
    }

    case r: ShutdownReq => {
      if (nodes.isEmpty)
        sender ! true
      else {
        shutdownSender = Some(sender)
        nodes.values.foreach(closeNodeConn(_, r.timeout))
      }
    }

    case r: RetryRequest => {
      processReq(r.req, r.sender)
    }

    case r: ClusterRequest => {
      processReq(r, sender)
    }

    case r: RequestFailed => {
      requests.remove(r.reqId).map {
        req =>
          requestsCount -= 1
          nodes.get(req.nodeId).foreach(_.fail(r, req))
          r.reason match {
            case NodeStopped | NodeOverloaded if req.req.retryCount > 0 && shutdownSender.isEmpty =>
              req.req.retryCount -= 1
              self ! RetryRequest(req.req, req.sender)
            case _ => req.sender ! r
          }
      }.getOrElse(log.warn(s"no req for fail resp $r"))
    }

    case r: ClusterResponse =>
      requests.remove(r.reqId).map {
        req =>
          requestsCount -= 1
          nodes.get(req.nodeId).foreach(_.finishReq(req))
          req.sender ! r
      }.getOrElse(log.warn(s"no req for success resp ${r.toString.take(500)}"))

    case CheckTimeout =>
      val currentTime = Platform.currentTime
      requests.values.filter(req => req.startTime + req.req.nodeTimeout < currentTime).foreach {
        req =>
          requestsCount -= 1
          requests.remove(req.req.reqId)
          nodes.get(req.nodeId).foreach(_.timeout(req))
          req.sender ! RequestFailed(req.req.reqId, Timeout)

          if (req.req.retryCount <= 0)
            req.sender ! RequestFailed(req.req.reqId, Timeout)
          else {
            req.req.retryCount -= 1

            self ! RetryRequest(req.req, req.sender)
          }

          log.debug(s"skip send package, due to timeout ${req.req.nodeTimeout} ms    \n${req.toString.take(500)}")
      }

    case r: NodeConnected => {
      nodes.get(r.nodeId).map(node => node.started = true)
    }

    case r: NodeDisconnected => {
      nodes.get(r.nodeId).map(node => node.started = false)
    }

    case r: NodeConnClosed => {
      nodes.remove(r.nodeId).foreach {
        node =>
          requestsCount -= 1
          requests.values.filter(_.nodeId == r.nodeId).foreach {
            r =>
              self ! RequestFailed(r.req.reqId, NodeStopped)
          }
      }
      shutdownSender.foreach(actor => if (requests.isEmpty) {
        timeoutTask.cancel()
        actor ! true
      })
    }

    case r => log.error(s"unknown request ${r.toString.take(500)}")
  }


  def processReq(r: ClusterRequest, sender: ActorRef) {
    if (shutdownSender.isDefined) {
      sender ! RequestFailed(r.reqId, Stopped)
    } else {

      Stats.time("req/select_node")(r.selectNode(nodes.values.filter(_.started))).map {
        node =>
          if (requests.contains(r.reqId))
            sender ! RequestFailed(r.reqId, DoubleSend)
          else {
            requestsCount += 1
            r match {
              case x:LoggableRequest => log.info("Req: " + x + " submit -> " + node.nodeId)
              case _ =>
            }
            node.startReq(r)
            requests += r.reqId -> RequestWithTimeout2(r, node.nodeId, sender)
            node.actor ! r
          }
      }.getOrElse {
        if (r.retryCount == 0) {
          sender ! RequestFailed(r.reqId, NoAvailableNode)
        } else {

          log.warn("Retry package, due to noNodeAvailable")
          r.retryCount = r.retryCount - 1
          context.system.scheduler.scheduleOnce((Math.max(5 - r.retryCount, 1) * 5).seconds, self, RetryRequest(r, sender))
        }
      }
    }
  }

  def closeNodeConn(node: NodeStatus, timeout: FiniteDuration): Any = {
    node.started = false
//    if (node.writeProcessing > 0)
//      context.system.scheduler.scheduleOnce(timeout, node.actor, CloseConnection)
//    else
    node.actor ! CloseConnection
  }


  override val supervisorStrategy = OneForOneStrategy() {
    case e: Exception ⇒ {
      log.error("akka error", e)
      SupervisorStrategy.Resume
    }
  }

  object CheckTimeout

}

case class RequestWithTimeout2(req: ClusterRequest, nodeId: Int, sender: ActorRef, startTime: Long = Platform.currentTime)
case class RetryRequest(req: ClusterRequest, sender: ActorRef)

trait NodeStatus{

  def nodeId: Int
  var started = false
  def actor: ActorRef
  def getMeta: NodeMeta
  def setMeta(meta: NodeMeta)

  def startReq(r: ClusterRequest)  : Unit =  {}
  def finishReq(r: RequestWithTimeout2)  : Unit =  {}
  def fail(f: RequestFailed, r: RequestWithTimeout2) : Unit = {}
  def timeout(r: RequestWithTimeout2) : Unit =  {}

}

class DHTNodeStatus(var meta: DHTNodeMeta, val actor: ActorRef) extends NodeStatus{
  val nodeId = meta.id

  def getMeta = meta

  def setMeta(meta: NodeMeta) {
    this.meta = meta.asInstanceOf[DHTNodeMeta]
  }
}

class DataNodeStatus(var meta: DataNodeMeta, val actor: ActorRef) extends NodeStatus{

  val nodeId = meta.id
  var writeProcessing: Int = 0
  var readProcessing: Int = 0
  var longProcessing: Int = 0
  private var timeouts: Int = 0
  private var fails: Int = 0

  override def startReq(r: ClusterRequest)  : Unit = {
    Stats.incr(s"node/${r.getClass.getName}/$nodeId/sent_count")
    r match {
      case wr: WriteRequest => writeProcessing += 1
      case rr: ReadRequest => readProcessing += 1
      case lr => longProcessing += 1
    }

  }

  override def finishReq(r: RequestWithTimeout2)  : Unit = {
//    Stats.addMetric(s"node/${r.req.getClass.getName}/$nodeId/req_time", Platform.currentTime - r.startTime toInt)
    r.req match {
      case wr: WriteRequest => writeProcessing -= 1
      case rr: ReadRequest => readProcessing -= 1
      case lr => longProcessing -= 1
    }

  }

  override def fail(f: RequestFailed, r: RequestWithTimeout2)  : Unit =  {
    Stats.incr(s"node/${f.reason}/$nodeId/req_fail")
    fails += 1
    finishReq(r)
  }

  override def timeout(r: RequestWithTimeout2)  : Unit =  {
    Stats.incr(s"node/${r.getClass.getName}/$nodeId/queue_timeout")
    timeouts += 1
    finishReq(r)
  }

  override def equals(obj: scala.Any) = {
    obj match {
      case r: DataNodeStatus => nodeId == r.nodeId
      case _ => false
    }
  }

  override def hashCode() = nodeId & 0x7fffffff

  def setMeta(meta: NodeMeta) {
    this.meta = meta.asInstanceOf[DataNodeMeta]
  }

  def getMeta = meta
}

case class RemoveNode(nodeId: Int, timeout: FiniteDuration)

object GetQueueSize

object GetActiveNodes


object ReqReadOrdering extends Ordering[DataNodeStatus] {
  def compare(x: DataNodeStatus, y: DataNodeStatus) = {
    y.meta.chunkReadTime * y.readProcessing - x.meta.chunkReadTime * x.readProcessing
  }
}

object ReqReadLongOrdering extends Ordering[DataNodeStatus] {
  def compare(x: DataNodeStatus, y: DataNodeStatus) = {
    y.meta.chunkReadTime * (y.readProcessing + y.longProcessing) - x.meta.chunkReadTime * (x.readProcessing + x.longProcessing)
  }
}



