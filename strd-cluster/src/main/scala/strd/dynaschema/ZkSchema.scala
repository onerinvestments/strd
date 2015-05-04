package strd.dynaschema

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryNTimes
import strd.cluster.proto.StrdCluster.DynaSchemaMeta

/**
 *
 * User: light
 * Date: 16/04/14
 * Time: 16:53
 */
object ZkSchema {
  val zkPrefix = "/dynaschema"

}

case class ZkSchema(curator: CuratorFramework, zkPath : String ) {
//  val zkPath = ZkSchema.zkPrefix + "/" + schemaType

  def publishSchema(  schemaName : String, schemaVersion : String, protoClasses  : Seq[String]) {
    val data = DynaSchemaMeta.newBuilder()
    .setTimestamp(System.currentTimeMillis())
    .setUser( Option( System.getProperty("user.name")).getOrElse{ throw new RuntimeException("can not get system userName")} )
    .setVersion( schemaVersion )
    .setSchemaName(schemaName)

    protoClasses.foreach(data.addProtoClass)

    if (curator.checkExists().forPath(zkPath) != null) {
      curator.setData().forPath(zkPath, data.build().toByteArray )
    } else {
      curator.create().creatingParentsIfNeeded().forPath(zkPath, data.build().toByteArray)
    }
  }

  def getVersion: Option[DynaSchemaMeta] = {
    if (curator.checkExists().forPath(zkPath) != null) {
      val data = curator.getData.forPath(zkPath)
      if (data == null) {
        None
      } else {
        Some(DynaSchemaMeta.parseFrom(data))
      }
    } else {
      None
    }
  }
}
