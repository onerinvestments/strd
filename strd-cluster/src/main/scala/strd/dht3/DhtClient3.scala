package strd.dht3

import strd.net.{ClusterConnector, StrdProtos}
import com.escalatesoft.subcut.inject.{BindingModule, Injectable}
import com.twitter.ostrich.admin.Service
import org.slf4j.LoggerFactory
import strd.util.{CTime, Stats2, ClusterConfigs}
import strd.dht.{BIN, DHTRegistry}
import com.google.protobuf.Message
import strd.CByte
import lmbrd.zn.util.MD5Util
import strd.dht3.proto.StrdDhtCluster
import strd.dht3.proto.StrdDhtCluster._

import strd.trace.PoolContext
import strd.concurrent.Promise
import java.util.{TimerTask, Timer}
import strd.cluster.proto.StrdCluster.{PingRequest, PingResponse}
import com.twitter.ostrich.stats.Stats


/**
 *
 * User: light
 * Date: 15/04/14
 * Time: 12:21
 */

trait DhtClient3 {
  def dhtConnector : Dht3ClientApi
}

class DhtClient3Impl(val dhtGroup : String)(implicit val bindingModule: BindingModule) extends Injectable with Service with DhtClient3 {


  implicit val conf = inject[ClusterConfigs]

  val log = LoggerFactory.getLogger(getClass)

  val registry      = new DHTRegistry( ClusterConfigs.cluster.get + "_dht3", conf)
    //inject[DHTRegistry]
  val protos        = StrdProtos.build("dht3_client", classOf[StrdDhtCluster])
  val connector     = new ClusterConnector( protos, dhtGroup )

  val replicaCount  = Dht3Conf(_.DHT_REPLICA_CONT)
  val defaultR      = Dht3Conf(_.DHT_R)
  val defaultW      = Dht3Conf(_.DHT_W)

  implicit val executionContext = PoolContext.cachedExecutor()

  val timer = new Timer()

  override def shutdown() = {
    timer.cancel()
    connector.shutdown()
  }

  override def start() = {
    connector.start()
    timer.scheduleAtFixedRate( new TimerTask() {
      override def run() = {
        connector.getGroup.foreach( c => {
          val time = CTime.now
          c.send[PingResponse]( PingRequest.newBuilder().setTime(time).build()).map( x => {
            Stats.addMetric("ping/dht3/" + c.meta.getHostString,  (CTime.now - x.getTime).toInt)
          })
        })
      }
    }, 5000L, 5000L)

  }

  val dhtConnector = new Dht3ClientApi {

    override def nodes: Seq[Int] = registry.ring.values.toSeq.map(_.nodeId)

    override def ringLookup(key: BIN) = {
      val re = registry.ring.find( CByte(MD5Util.md5Bytes(key)) )
      // remove unavailable nodes
      re.next.flatMap( connector.getNode ).map(x => x.meta.getNodeId )
    }

    override def requestNode(id: Int, req: DhtDynamicRequest, timeout: Long) = {
      //Stats2.incr()
//      println(s"$id: $req")

      connector.getNode(id).map( n =>
        Stats2.futureTime(s"dht3/request/${req.getTableId}/$id")( n.send[Message](req, timeout) ) ).getOrElse{
        val p = Promise[DhtDynamicResponse]()
        p.failure( new RuntimeException("Node not available: " + id))
        p.future
      }
    }

    override def defaultW = DhtClient3Impl.this.defaultW

    override def defaultR = DhtClient3Impl.this.defaultR

    override def log = DhtClient3Impl.this.log

    override def replicaCount = DhtClient3Impl.this.replicaCount

    override def notifyNode(id: Int, req: DhtDynamicRequest) {
      connector.getNode(id).map{ c => c.sendWithoutReply(req) }.getOrElse {
        log.warn(s"Can not notify node -> node $id is not found")
      }
    }

  }




}
