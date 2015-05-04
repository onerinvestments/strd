package strd.client

import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import com.twitter.ostrich.admin.Service
import scala.concurrent.{Await, ExecutionContext}
import akka.actor.{ActorRef, Props, ActorSystem}
import org.apache.curator.framework.CuratorFramework
import strd.util.ClusterConfigs
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener, PathChildrenCache}
import strd.cluster.{NodeMeta, ShutdownReq}
import org.slf4j.LoggerFactory
import com.twitter.chill.ScalaKryoInstantiator
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import akka.util.Timeout
import concurrent.duration._
import com.twitter.ostrich.stats.Stats
import akka.pattern.ask
import java.util.{TimerTask, Timer}
import lmbrd.zn.util.TimeUtil

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 8/19/13
 * Time: 3:50 PM
 */
class NodesClient(nodesPath: String,
                  statusFactory: (NodeMeta, ActorRef) => NodeStatus)(implicit val bindingModule: BindingModule) extends Injectable with Service {

  implicit val executionContext = inject[ExecutionContext]
  //val nodes = new PriorityBlockingQueue[NodeRating]()
  val system = ActorSystem("clients")

  val conf = inject[ClusterConfigs]
  val nodeId = conf.nodeId.get

  val curator = inject[CuratorFramework]
  val nodesCache = new PathChildrenCache(curator, nodesPath, true)

  val dispatcher = system.actorOf(Props[ClientDispatcher2]( new ClientDispatcher2 ))

  val kryoPool = ScalaKryoInstantiator.defaultPool

  val log = LoggerFactory.getLogger(getClass)


  Stats.addGauge(s"activeNodes$nodesPath") {
    getNodesCount()
  }


  def getNodesCount() = {
    try {
      implicit val timeout = Timeout(1 seconds)
      Await.result(dispatcher ? GetActiveNodes, 1.seconds).asInstanceOf[Seq[NodeStatus]].count(_.started == true)
    } catch {
      case t: Exception => 0
    }

  }
  val shutdownTimer = new Timer()
  val purgeConnectTimer = new Timer()

  def start() {

    log.debug("\n\n\n\nnodes client start\n\n\n")
    import collection.JavaConverters._
    purgeConnectTimer.schedule(new TimerTask {
      override def run() = {
        try {
          implicit val timeout: Timeout = Timeout(1 minute)
          val nodes = Await.result(dispatcher ? GetActiveNodes, 1.seconds).asInstanceOf[Seq[NodeStatus]]

          curator.getChildren.forPath(nodesPath).asScala.foreach{ ch => {
            val child = curator.getData.forPath(nodesPath +"/" + ch)

            val meta = kryoPool.fromBytes(child).asInstanceOf[NodeMeta]

            if (meta.id != nodeId && nodes.find(_.nodeId == meta.id).isEmpty ){
              log.warn("Adding node from purge! : " + meta)
              val node = statusFactory(meta, system.actorOf( Props[NodeConnector]( new NodeConnector(dispatcher, meta.nodeType +"_" + meta.id))) )
              dispatcher ! node
            }
          }}

        } catch {case x:Exception => log.error("",x)}
      }
    },3000,3000)

    nodesCache.getListenable.addListener(new PathChildrenCacheListener {
      def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent) {
        event.getType match {
          case PathChildrenCacheEvent.Type.CHILD_ADDED =>
            val meta = kryoPool.fromBytes(event.getData.getData).asInstanceOf[NodeMeta]
            if (meta.id == nodeId)
              log.debug(s"SELF node $nodeId registered as ${meta.host}:${meta.port}")
            else {
              log.debug( s"adding node ${meta.id} at ${meta.host}:${meta.port} type: " +meta.nodeType)
              val node = statusFactory(meta, system.actorOf( Props[NodeConnector]( new NodeConnector(dispatcher, meta.nodeType +"_" + meta.id))) )
              dispatcher ! node
            }
          case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
            val meta = kryoPool.fromBytes(event.getData.getData)
            dispatcher ! meta
            //log.debug("update mode " + meta.id)
          case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
            val meta = kryoPool.fromBytes(event.getData.getData).asInstanceOf[NodeMeta]
            log.debug(s"removing node ${meta.id} at ${meta.host}:${meta.port}")

            if (meta.id == nodeId) {
              Stats.incr("zk/self_unregistered/" + nodeId)
              log.debug(s"SELF node $nodeId unregistered, scheduling SHUTDOWN after 15 seconds")

              shutdownTimer.schedule( new TimerTask {
                override def run() = {
                  log.info("Checking self node registration in zk")
                  val stat = curator.checkExists().forPath( nodesPath + "/" + nodeId )
                  if ( stat != null) {
                    Stats.incr("zk/abort_shutdown/" + nodeId)
                    log.warn( s"SELF node $nodeId unregisteration aborted due to NODE exists in zk" )
                  } else {
                    Stats.incr("zk/shutdown/" + nodeId)
                      log.error( "\nSystem.exit call...\n" )
                      Thread.sleep( 1000 )
                      System.exit( -2 )
                  }
                }
              }, TimeUtil.aSECOND * 15 )

            } else {
              dispatcher ! RemoveNode(meta.id, 30 seconds)
            }

          case typ => log.debug(s"nodes cache event $typ")
        }
      }
    })


    nodesCache.start(StartMode.NORMAL)
    log.debug(s"init cache ${nodesCache.getCurrentData.size()}")
  }

  override def quiesce() {

  }

  def shutdown() {
    purgeConnectTimer.cancel()
    import akka.pattern.ask
    implicit val timeout: Timeout = Timeout(1 minute)

    nodesCache.close()
    Await.result(dispatcher ? ShutdownReq(timeout.duration), timeout.duration)

  }

}
