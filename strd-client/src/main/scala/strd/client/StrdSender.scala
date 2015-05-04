package strd.client

import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import com.twitter.ostrich.admin.Service
import java.util.concurrent.ConcurrentHashMap
import akka.actor._
import org.apache.curator.framework.CuratorFramework
import strd.util.{Stats2, MonotonicallyIncreaseGenerator, ZkUtils}
import com.twitter.chill.ScalaKryoInstantiator
import org.slf4j.LoggerFactory
import collection.convert.decorateAll._
import scala.concurrent.{Future, ExecutionContext}
import strd.cluster._
import strd.core.StrdTopology
import java.util.concurrent.locks.ReentrantLock
import akka.pattern.ask
import akka.util.Timeout
import strd.core.StrdTopology
import concurrent.duration._
import strd.cluster.RequestFailed
import scala.Some
import strd.core.StrdTopology
import strd.cluster.StrdRequest
import strd.cluster.ClientResponse
import strd.cluster.ShutdownReq

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 8/13/13
 * Time: 2:21 PM
 */
class StrdSender(implicit val bindingModule: BindingModule) extends Injectable with Service {

  implicit val executionContext = inject[ExecutionContext]
  //val nodes = new PriorityBlockingQueue[NodeRating]()
  val system = ActorSystem("clients")

  val curator = inject[CuratorFramework]
  val zkUtils = inject[ZkUtils]
  val tablesMap = new ConcurrentHashMap[Int, ActorRef]()
  val kryoPool = ScalaKryoInstantiator.defaultPool
  val log = LoggerFactory.getLogger(getClass)
  val nodesClient = inject[NodesClient]('dataNodes)

  //  Stats.addGauge("dispatcher/queued_packages") {
  //    qPackage.get()
  //  }


  def start() {
  }

  val lock = new ReentrantLock()

  def removeTopology(name: String) {
    if (curator.checkExists().forPath(zkUtils.topologyPath(name)) != null) {
      curator.delete().forPath(zkUtils.topologyPath(name))
    }
  }

  def deployRemoteTopo(proj: (StrdTopology[_, _], StrdTopology[_, _]), update: Boolean) {
    deployRemoteTopo(proj._1, update)
    deployRemoteTopo(proj._2, update)
  }

  def deployRemoteTopo(topology: StrdTopology[_, _], update: Boolean) {
    log.debug("-> deploy:" + topology.topologyId)

    if (update) {
      if (curator.checkExists().forPath(zkUtils.topologyPath(topology.topologyId)) != null) {
        curator.setData().forPath(zkUtils.topologyPath(topology.topologyId), kryoPool.toBytesWithoutClass(topology))
      } else
        curator.create().creatingParentsIfNeeded().forPath(zkUtils.topologyPath(topology.topologyId), kryoPool.toBytesWithoutClass(topology))
    } else {
      curator.create().creatingParentsIfNeeded().forPath(zkUtils.topologyPath(topology.topologyId), kryoPool.toBytesWithoutClass(topology))
    }
  }

/*
  def sendEvent(table: Int, event: AnyRef) = {
    tableSender(table) ! event
  }

  def sendEvents(table: Int, events: Seq[_]) = {
    tableSender(table) ! events
  }


  def tableSender(table: Int): ActorRef = {
    Option(tablesMap.get(table)).getOrElse {
      lock.lock()
      val actor = try {
        Option(tablesMap.get(table)).getOrElse {
          val a = system.actorOf(Props(new TableSender(table, nodesClient.dispatcher)))
          tablesMap.put(table, a)
          a
        }
      } finally {
        lock.unlock()
      }
      actor

    }
  }
*/


  def sendRequest(req: StrdRequest[_])(implicit timeout: Timeout = Timeout(req.nodeTimeout * (req.retryCount + 1))): Future[ClusterResponse] = {

    nodesClient.dispatcher ? req map {
      case resp: ClusterResponse => resp
      case r => RequestFailed(req.reqId, FailReason.Unknown, Some(s"unknown response $r"))
    }
  }

  def sendRequest[REQ, RESP](topo: StrdTopology[REQ, RESP], req: REQ)(implicit timeout: Timeout = Timeout(30.seconds)): Future[Seq[RESP]] =
    Stats2.futureTime(s"client/req/${topo.topologyId}") {
      sendRequest(StrdRequest(MonotonicallyIncreaseGenerator.nextId(), topo.topologyId, req, timeout.duration.toMillis)).map {
        case resp: ClientResponse => resp.value.asInstanceOf[Seq[RESP]]
        case f: RequestFailed => throw new RuntimeException(s"${f.reason}: ${f.message}")
      }
    }

  override def quiesce() {

  }

  def shutdown() {
    tablesMap.values().asScala.foreach(_ ! ShutdownReq)
  }

}


