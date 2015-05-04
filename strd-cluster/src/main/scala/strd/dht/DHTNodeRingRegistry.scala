package strd.dht

import DhtDialect._
import org.slf4j.LoggerFactory
import strd.util.ClusterConfigs
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import com.twitter.ostrich.admin.Service

/**
 *
 * User: light
 * Date: 9/29/13
 * Time: 6:27 AM
 */

class DHTNodeRingRegistry(cluster : String)(implicit val bindingModule: BindingModule) extends Injectable with Service {

  val log = LoggerFactory.getLogger(getClass)
  implicit val conf = inject[ClusterConfigs]
  val nodeId = conf.nodeId.get

  val nodesCount = DHTConf(_.NODES_COUNT)

  def tryInit() {
    inTransaction {

      val tokenVersions = from(DHTPgSchema.tokens)(t => where(t.cluster === cluster) select t).toIndexedSeq.groupBy(_.version)
      if (tokenVersions.isEmpty) {
        throw new IllegalStateException("No tokens found in registry")
      }
      // Get tokens for the last cluster version
      val tokens = tokenVersions.get(tokenVersions.keys.toSeq.sorted.last).get.sortBy(_.key)
      val version = tokens.headOption.map(_.version)

      log.debug(s"DHT Tokens: ${tokens.size}  unassigned:${tokens.count(_.node == 0)} version: $version}")

      if (!tokens.exists(_.node == nodeId) && tokens.exists(_.node == 0) ) {


        log.debug(s"First run -> initiaiting tokens for $nodesCount nodes")

        var toMe : Int = tokens.size / nodesCount

        val free = tokens.filter(_.node == 0)

        if(free.size > toMe && free.size < toMe * 2) {
          toMe = free.size
        }

        if (free.size < toMe) {
          throw new RuntimeException(s"Unassigned tokens: $free")
        }

        val toMeIds = free.take(toMe).map(_.key).toIndexedSeq

        val c = update(DHTPgSchema.tokens)(t => where(t.key in (toMeIds) and t.cluster === cluster and t.version === version.get) set (t.node := nodeId))
        if ( c != toMe) {
          throw new IllegalStateException(s"Updated tokens count $c does not match expected $toMe")
        }
        log.info(s"DHT TOKENS Assigned: $c to $nodeId")

      } else {
        val myTokens = tokens.count(_.node == nodeId)
        log.info(s"My tokens : $myTokens")

        if (myTokens == 0) {
          log.error("NO TOKENS IN REGISTRY ==> SHUTDOWN NODE")
          Thread.sleep(1000)
          System.exit(6)
        }
      }
    }
  }

  def shutdown() {

  }

  def start() {
    tryInit()
  }

}


