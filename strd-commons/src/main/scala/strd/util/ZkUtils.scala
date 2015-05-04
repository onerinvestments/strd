package strd.util

import com.escalatesoft.subcut.inject.{BindingModule, Injectable}
import com.twitter.ostrich.admin.Service
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.utils.ZKPaths
import strd.common.ExZookeeperFactory

/**
 * $Id$
 * $URL$
 * User: bulay
 * Date: 7/29/13
 * Time: 4:35 PM
 */
class ZkUtils(val clusterName: String) {

  val chunkBaseSerialPath = ZKPaths.makePath(clusterName, "chunk_serial")
  val indexesPath = ZKPaths.makePath(clusterName, "indexes")
  val topologiesPath = ZKPaths.makePath(clusterName, "topologies")
  val dataNodesPath = ZKPaths.makePath(clusterName, "data_nodes")
  val dhtNodesPath = ZKPaths.makePath(clusterName, "dht_nodes")
  val masterPath = ZKPaths.makePath(clusterName, "master")
  val gwMasterPath = ZKPaths.makePath(clusterName, "gw_master")
  val tablesPath = ZKPaths.makePath(clusterName, "strd_tables")

  def chunkSerialPath(table: Int) = ZKPaths.makePath(chunkBaseSerialPath, table.toString)

  def indexPath(table: String, fieldId: Int) = ZKPaths.makePath(indexesPath, s"${table}_$fieldId")

  def topologyPath(topologyId: String) = ZKPaths.makePath(topologiesPath, topologyId)

  def dataNodePath(nodeId: Int) = ZKPaths.makePath(dataNodesPath, nodeId.toString)

  def dhtNodePath(nodeId: Int) = ZKPaths.makePath(dhtNodesPath, nodeId.toString)

  def tablePath(table: Int) = ZKPaths.makePath(tablesPath, "table" + table)

//  def chunkPath(chunkId: ChunkId) = {
//    ZKPaths.makePath(tablePath(chunkId.table), "" + (chunkId.seq / 1000) + "/" + chunkId.compress())
//  }
//
//  def chunksReplicasPath(chunkId: ChunkId, nodeId: Int) = ZKPaths.makePath(chunkPath(chunkId), nodeId.toString)

}

trait CuratorConfSimple { this : StrdService with ClusterStrdConf =>
  println("--> starting Curator: " + StrdNodeConf(_.ZK_ADDR) )
  val curator = CuratorFrameworkFactory.builder()
    .zookeeperFactory( new ExZookeeperFactory )
    .connectString(StrdNodeConf(_.ZK_ADDR))
    .sessionTimeoutMs(30000)
    .connectionTimeoutMs(15000)
    .retryPolicy(new RetryNTimes(Integer.MAX_VALUE - 1, 1000))
    .build()

  curator.start()
  bind[CuratorFramework]      toSingle curator
  println("<-- curator")
}

trait CuratorStrdConf { this : StrdService with ClusterStrdConf =>
  println("--> CuratorStrdConf")

  lazy val zkUtils = new ZkUtils(ClusterConfigs.cluster.get)

  val c = CuratorFrameworkFactory.builder()
      .zookeeperFactory( new ExZookeeperFactory )
      .connectString(StrdNodeConf(_.ZK_ADDR))
      .sessionTimeoutMs(30000)
      .connectionTimeoutMs(15000)
      .retryPolicy(new RetryNTimes(Integer.MAX_VALUE - 1, 1000))
      .build()
  c.start()

  bind[CuratorFramework]      toSingle c
  bind[CuratorService]        toSingle new CuratorService
  bind[ZkUtils]               toSingle zkUtils
}


class CuratorService(implicit val bindingModule:BindingModule) extends Service with Injectable {
  val curator = inject[CuratorFramework]

  override def quiesce() {}

  def shutdown() = {
    curator.close()
  }
  def start() = {
  }
}

