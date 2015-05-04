package strd.net

import com.twitter.ostrich.stats.Stats
import org.apache.curator.framework.CuratorFramework
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import com.twitter.ostrich.admin.Service
import java.util.{TimerTask, Timer}
import strd.cluster.proto.StrdCluster.NodeMeta

/**
 *
 * User: light
 * Date: 29/04/14
 * Time: 17:34
 */

class ClusterGroupStats(path: String, groupId: String)(implicit val bindingModule: BindingModule) extends Injectable with Service {

  import collection.JavaConverters._

  val curator = inject[CuratorFramework]

  Stats.addGauge(s"!meta/$groupId/nodes") {
    curator.getChildren.forPath(path).size()
  }

  val timer = new Timer

  override def shutdown() = {
    timer.cancel()
  }

  override def start() = {
    timer.scheduleAtFixedRate(new TimerTask() {
      override def run() = {
        try {
          val children = curator.getChildren.forPath(path).asScala
          Stats.setLabel(s"!meta/$groupId/clusterNodes", "[" + children.mkString(":") + "]")


          children.foreach(ch => {
            val m = NodeMeta.parseFrom(curator.getData.forPath(path + "/" + ch))
            val str = s"${m.getHost}:${m.getNodePort},  w:${m.getWeight},  d:${m.getDecommission},  n:${m.getHostString}"
            Stats.setLabel(s"!meta/${m.getGroupId}/$ch", str)
          })

        } catch {
          case ignored: Exception =>
        }

      }
    }, 3000, 5000)
  }
}
