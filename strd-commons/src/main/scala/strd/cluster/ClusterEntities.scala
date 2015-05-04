package strd.cluster

import strd.ChunkSet
import strd.client._
import strd.util.Utils

import scala.concurrent.duration._
import scala.util.Random

/**
 *
 * User: light
 * Date: 8/14/13
 * Time: 1:13 PM
 */

object ClusterEntities {

}

trait NodeMeta {
  def id: Int

  def host: String

  def port: Int

  def nodeType: NodeType

  def isDecommissioning: Boolean
}

sealed trait NodeType

object NodeType {

  case object DATA_NODE extends NodeType

  case object DHT_NODE extends NodeType


  def randomByWeight[X](nodes: Iterable[X], weight: X => Double): Option[X] = {
    val rnd = Random.nextDouble() * nodes.map(weight).sum
    (rnd /: nodes)((r, dn) => {
      val w = weight(dn)
      if (r < w) return Some(dn) else r - w
    })
    None
  }

}

case class DataNodeMeta(id: Int,
                        host: String,
                        port: Int,
                        freeSpace: Long = 0,
                        chunksCount: Int = 0,
                        chunkReadTime: Int = 1,
                        packageWriteTime: Int = 1,
                        readFail: Int = 0,
                        packageWriteFail: Int = 0,
                        isDecommissioning: Boolean = false
                         ) extends NodeMeta {
  def nodeType = NodeType.DATA_NODE

}

case class DHTNodeMeta(id: Int,
                       host: String,
                       port: Int,
                       isDecommissioning: Boolean = false
                        ) extends NodeMeta {
  def nodeType = NodeType.DHT_NODE
}

case class PackageSaved(reqId: Long) extends ClusterResponse

case class PackageTimeout(pkgId: Long)

case class StrdRequest[T](reqId: Long,
                          topologyId: String,
                          content: T,
                          nodeTimeout: Long,
                          traceLevel: Int = 0) extends LongRequest {
}

case class ClientResponse(reqId: Long, value: Seq[_], count: Int) extends ClusterResponse {
  override def toString = s"${getClass.getName} reqId:$reqId  value:$value.size}"
}

//case class RequestFailed(reqId: Long, reason: String)
case class RequestFailed(reqId: Long, reason: FailReason, message: Option[String] = None) extends ClusterResponse

trait FailReason {

}

object FailReason {

  case object NodeStopped extends FailReason

  case object NoAvailableNode extends FailReason

  case object Stopped extends FailReason

  case object DoubleSend extends FailReason

  case object NodeOverloaded extends FailReason

  case object Timeout extends FailReason

  case object Unknown extends FailReason

  case object BadRequest extends FailReason

}

case class RequestCompleted[RES](reqId: Long, result: RES)

trait NodeRequest extends ClusterRequest {
  retryCount = 0

  def nodeId: Int

  def selectNode(nodes: Iterable[NodeStatus]) = nodes.find(_.nodeId == nodeId)
}

trait ClusterRequest {


  def reqId: Long

  def nodeTimeout: Long

  var retryCount: Int = 0

  def selectNode(nodes: Iterable[NodeStatus]): Option[NodeStatus]

  def timeout = nodeTimeout * (retryCount + 1)

  var sentTime :Long = 0
  val creationTime  = System.currentTimeMillis()
}

trait LoggableRequest

trait ReadRequest extends ClusterRequest {

  def chunkIds: ChunkSet
  def chunksCount = chunkIds.size

}

trait WriteRequest extends ClusterRequest {
  retryCount = 3



  def selectNode(nodes: Iterable[NodeStatus]) = NodeType.randomByWeight[NodeStatus](
    nodes.filterNot(_.getMeta.isDecommissioning),
    x => Utils.freeSpaceWeight(x.asInstanceOf[DataNodeStatus].meta.freeSpace)
  )

  def eventsCount: Int

}

trait LongRequest extends ClusterRequest {

  def selectNode(nodes: Iterable[NodeStatus]) = NodeType.randomByWeight[NodeStatus](nodes.filterNot(_.getMeta.isDecommissioning), x => 1)
}

trait ClusterResponse extends BinarySizeInfo{
  def reqId: Long

  var sentTime : Long = 0
  val creationTime  = System.currentTimeMillis()

}

trait BinarySizeInfo {
  @transient var bytesSize : Long = 0L
}


case class NodeConnected(nodeId: Int)

case class NodeDisconnected(nodeId: Int)

case class NodeConnClosed(nodeId: Int)

object CloseConnection

case class ShutdownReq(timeout: FiniteDuration)

case class TableMeta(id: Int, repFactor: Int, name: Option[String] = None)

